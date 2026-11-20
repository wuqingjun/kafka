/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright 2026 qjwu. Part of the multi-identity extensions of this fork (see README.md, "Modifications").

/**
 * librdkafka client authenticating with a SAML token over SASL/OAUTHBEARER (the broker side is this fork's
 * SamlOAuthBearerValidatorCallbackHandler), refreshing the token from a file and keeping its connections alive across
 * token expiries.
 *
 * How it works
 *   - Something outside Kafka keeps a fresh SAML assertion, base64-encoded, in a file (an agent / sidecar talking to
 *     the identity provider: STS / WS-Trust, ECP, Kerberos, service accounts...). SAML has no refresh token.
 *   - librdkafka calls the oauthbearer_token_refresh_cb at start and then at 80 % of the token's remaining lifetime.
 *     The callback re-reads the file and hands the token to librdkafka with its REAL expiry: the earliest
 *     NotOnOrAfter in the decoded assertion (a text scan, no XML parsing, no trust decision — the broker validates
 *     the token). An already expired token is not used: rd_kafka_oauthbearer_set_token_failure() makes librdkafka
 *     retry in 10 s, picking up a newly written file.
 *   - At login the broker returns the session lifetime (until the token expires). librdkafka >= 2.2.0 (KIP-368)
 *     re-authenticates each open connection before then with the latest token — no disconnect. Older versions are
 *     disconnected at token expiry and reconnect with the new token.
 *   Passing the wrong lifetime (e.g. "1 hour from now" for a token read from a file that is already 20 minutes old)
 *   makes the refresh come too late: re-authentication with an expired token fails.
 *
 * Usage:
 *   saml_oauthbearer -b <brokers> -t <topic> -f <token file> [-r <msgs/s>] [-d <seconds>]
 *                    [-l <fallback lifetime s>] [-X <property>=<value> ...]
 *
 * Example:
 *   saml_oauthbearer -b broker1:9094 -t test -f /run/saml/token.b64 -r 2 -d 600 \
 *     -X security.protocol=sasl_ssl -X ssl.ca.location=root-ca.crt
 *
 * Build against an installed librdkafka (uses its OpenSSL for base64):
 *   cc saml_oauthbearer.c -o saml_oauthbearer $(pkg-config --cflags --libs rdkafka libcrypto)
 */

#define _GNU_SOURCE
#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <openssl/evp.h>

#include <librdkafka/rdkafka.h>

static const char *token_file;
static long fallback_lifetime_s = 3600;
static volatile sig_atomic_t run = 1;
static long delivered, failed, refreshes;

static void stop(int sig) {
        run = 0;
}

static void ts(char *out, size_t size) {
        time_t t = time(NULL);
        strftime(out, size, "%H:%M:%S", localtime(&t));
}

/** Read the whole file, whitespace removed. Caller frees. */
static char *read_token(const char *path, char *errstr, size_t errstr_size) {
        FILE *f = fopen(path, "rb");
        char *buf, *out;
        long n;
        size_t i, j = 0;
        if (!f) {
                snprintf(errstr, errstr_size, "cannot open %s", path);
                return NULL;
        }
        fseek(f, 0, SEEK_END);
        n = ftell(f);
        fseek(f, 0, SEEK_SET);
        buf = malloc((size_t)n + 1);
        out = malloc((size_t)n + 1);
        n   = (long)fread(buf, 1, (size_t)n, f);
        fclose(f);
        for (i = 0; i < (size_t)n; i++)
                if (!isspace((unsigned char)buf[i]))
                        out[j++] = buf[i];
        out[j] = '\0';
        free(buf);
        if (j == 0) {
                free(out);
                snprintf(errstr, errstr_size, "%s is empty", path);
                return NULL;
        }
        return out;
}

/** "2026-11-20T15:04:05[.123][Z|+hh:mm]" -> epoch ms, or -1. */
static long long parse_iso8601_ms(const char *s) {
        struct tm tm = {0};
        int year, mon, day, hour, min, sec, n = 0, sign;
        long long ms = 0, off_s = 0;
        int oh, om;
        if (sscanf(s, "%4d-%2d-%2dT%2d:%2d:%2d%n", &year, &mon, &day, &hour, &min, &sec, &n) != 6)
                return -1;
        s += n;
        if (*s == '.') { /* fraction: keep milliseconds */
                int digits = 0;
                s++;
                while (isdigit((unsigned char)*s)) {
                        if (digits < 3)
                                ms = ms * 10 + (*s - '0');
                        digits++;
                        s++;
                }
                while (digits++ < 3)
                        ms *= 10;
        }
        if (*s == '+' || *s == '-') {
                sign = *s == '-' ? -1 : 1;
                if (sscanf(s + 1, "%2d:%2d", &oh, &om) != 2)
                        return -1;
                off_s = sign * (oh * 3600LL + om * 60LL);
        } else if (*s != 'Z' && *s != '\0') {
                return -1;
        }
        tm.tm_year = year - 1900;
        tm.tm_mon  = mon - 1;
        tm.tm_mday = day;
        tm.tm_hour = hour;
        tm.tm_min  = min;
        tm.tm_sec  = sec;
        return ((long long)timegm(&tm) - off_s) * 1000 + ms;
}

/** Earliest NotOnOrAfter in the base64-decoded token (epoch ms), or -1 if none. */
static long long earliest_not_on_or_after(const char *b64) {
        size_t len = strlen(b64);
        unsigned char *xml = malloc(len + 1);
        int n = EVP_DecodeBlock(xml, (const unsigned char *)b64, (int)len);
        long long earliest = -1;
        const char *p;
        if (n < 0) {
                free(xml);
                return -1;
        }
        xml[n] = '\0';
        for (p = (const char *)xml; (p = strstr(p, "NotOnOrAfter")) != NULL; p++) {
                const char *q = p + strlen("NotOnOrAfter");
                char value[64];
                size_t k = 0;
                char quote;
                long long t;
                while (*q == ' ')
                        q++;
                if (*q++ != '=')
                        continue;
                while (*q == ' ')
                        q++;
                quote = *q++;
                if (quote != '"' && quote != '\'')
                        continue;
                while (*q && *q != quote && k < sizeof(value) - 1)
                        value[k++] = *q++;
                value[k] = '\0';
                t        = parse_iso8601_ms(value);
                if (t > 0 && (earliest < 0 || t < earliest))
                        earliest = t;
        }
        free(xml);
        return earliest;
}

/** librdkafka asks for a (new) token: at start, then at 80 % of the remaining lifetime. */
static void refresh_cb(rd_kafka_t *rk, const char *oauthbearer_config, void *opaque) {
        char errstr[512], now_s[16];
        char *token = read_token(token_file, errstr, sizeof(errstr));
        long long now_ms = (long long)time(NULL) * 1000, expiry;
        ts(now_s, sizeof(now_s));
        refreshes++;
        if (!token) {
                fprintf(stderr, "%s REFRESH failed: %s\n", now_s, errstr);
                rd_kafka_oauthbearer_set_token_failure(rk, errstr);
                return;
        }
        expiry = earliest_not_on_or_after(token);
        if (expiry < 0) {
                expiry = now_ms + fallback_lifetime_s * 1000;
                fprintf(stderr, "%s REFRESH: token has no readable NotOnOrAfter, using %ld s\n", now_s,
                        fallback_lifetime_s);
        }
        if (expiry <= now_ms) {
                snprintf(errstr, sizeof(errstr), "token in %s expired %lld s ago", token_file,
                         (now_ms - expiry) / 1000);
                fprintf(stderr, "%s REFRESH: %s, waiting for a fresh one\n", now_s, errstr);
                rd_kafka_oauthbearer_set_token_failure(rk, errstr);
                free(token);
                return;
        }
        /* The principal name is client-side only; the broker uses the token's subject. */
        if (rd_kafka_oauthbearer_set_token(rk, token, expiry, "saml-token-holder", NULL, 0, errstr,
                                           sizeof(errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR) {
                fprintf(stderr, "%s REFRESH: set_token failed: %s\n", now_s, errstr);
                rd_kafka_oauthbearer_set_token_failure(rk, errstr);
        } else {
                fprintf(stderr, "%s REFRESH #%ld: token read from %s, expires in %lld s\n", now_s, refreshes,
                        token_file, (expiry - now_ms) / 1000);
        }
        free(token);
}

static void dr_cb(rd_kafka_t *rk, const rd_kafka_message_t *m, void *opaque) {
        if (m->err) {
                failed++;
                fprintf(stderr, "%% Delivery failed: %s\n", rd_kafka_err2str(m->err));
        } else {
                delivered++;
        }
}

static void usage(const char *prog) {
        fprintf(stderr, "Usage: %s -b <brokers> -t <topic> -f <token file> [-r <msgs/s>] [-d <seconds>] "
                        "[-l <fallback lifetime s>] [-X <property>=<value> ...]\n", prog);
        exit(1);
}

int main(int argc, char **argv) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *rk;
        char errstr[512];
        const char *brokers = NULL, *topic = NULL;
        int rate = 2, duration_s = 60, opt;
        long sent = 0;
        time_t start, last_report;

        while ((opt = getopt(argc, argv, "b:t:f:r:d:l:X:")) != -1) {
                switch (opt) {
                case 'b': brokers = optarg; break;
                case 't': topic = optarg; break;
                case 'f': token_file = optarg; break;
                case 'r': rate = atoi(optarg); break;
                case 'd': duration_s = atoi(optarg); break;
                case 'l': fallback_lifetime_s = atol(optarg); break;
                case 'X': {
                        char *name = strdup(optarg), *val = strchr(name, '=');
                        if (!val)
                                usage(argv[0]);
                        *val++ = '\0';
                        if (rd_kafka_conf_set(conf, name, val, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                                fprintf(stderr, "%% %s\n", errstr);
                                return 1;
                        }
                        free(name);
                        break;
                }
                default: usage(argv[0]);
                }
        }
        if (!brokers || !topic || !token_file || rate <= 0)
                usage(argv[0]);

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set(conf, "sasl.mechanisms", "OAUTHBEARER", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                return 1;
        }
        rd_kafka_conf_set_oauthbearer_token_refresh_cb(conf, refresh_cb);
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create producer: %s\n", errstr);
                return 1;
        }
        signal(SIGINT, stop);
        signal(SIGTERM, stop);

        start = last_report = time(NULL);
        while (run && time(NULL) - start < duration_s) {
                char payload[64];
                struct timespec now;
                long long next_ms;
                snprintf(payload, sizeof(payload), "saml-oauthbearer %ld", sent);
                if (rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_VALUE(payload, strlen(payload)),
                                      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_END) == 0)
                        sent++;
                /* Keep the rate: poll (delivery reports, token refresh callback) until the next send is due —
                 * rd_kafka_poll() returns early whenever an event was served. */
                clock_gettime(CLOCK_MONOTONIC, &now);
                next_ms = now.tv_sec * 1000LL + now.tv_nsec / 1000000 + 1000 / rate;
                for (;;) {
                        long long left;
                        clock_gettime(CLOCK_MONOTONIC, &now);
                        left = next_ms - (now.tv_sec * 1000LL + now.tv_nsec / 1000000);
                        if (left <= 0)
                                break;
                        rd_kafka_poll(rk, (int)left);
                }
                if (time(NULL) - last_report >= 60) {
                        char now_s[16];
                        ts(now_s, sizeof(now_s));
                        fprintf(stderr, "%s progress: sent %ld, delivered %ld, failed %ld\n", now_s, sent, delivered,
                                failed);
                        last_report = time(NULL);
                }
        }
        rd_kafka_flush(rk, 30 * 1000);
        fprintf(stderr, "RESULT: sent %ld, delivered %ld, failed %ld, token refreshes %ld\n", sent, delivered, failed,
                refreshes);
        rd_kafka_destroy(rk);
        return failed == 0 && delivered == sent ? 0 : 2;
}
