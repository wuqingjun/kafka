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
 * Verify the broker by the SHA-256 thumbprint of its root CA — no CA
 * certificate file on the client.
 *
 * The broker must send its full certificate chain: leaf -> intermediate CA(s)
 * -> root CA. OpenSSL calls the ssl.certificate.verify_cb once per
 * certificate, root first:
 *
 *   root          OpenSSL can't trust it (no CA file), x509_error =
 *                 X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN. We accept it only if
 *                 the SHA-256 of its DER encoding equals a pinned thumbprint.
 *                 We also require the match when OpenSSL already trusts the
 *                 top certificate (e.g. from a system CA store), so a CA that
 *                 happens to be installed can't bypass the pin.
 *   intermediate  OpenSSL has verified its signature with the root's public
 *                 key (and its dates, CA flag, path length): accept only if
 *                 x509_error == 0.
 *   leaf          Signature checked with the intermediate's key, dates, and
 *                 the hostname (ssl.endpoint.identification.algorithm=https):
 *                 accept only if x509_error == 0 and the root of this same
 *                 handshake matched.
 *
 * The thumbprint is public — it needs integrity (deliver it like other trusted
 * configuration), not secrecy. Several thumbprints (comma-separated) allow a
 * root CA rotation.
 *
 * Usage:
 *   root_ca_thumbprint -b <brokers> -t <topic> -s <sha256>[,<sha256>...]
 *                      [-X <librdkafka property>=<value> ...] [-m <message>]
 *
 * Example (mutual TLS, client certificate in PEM files):
 *   root_ca_thumbprint -b localhost:9092 -t test \
 *     -s 15476b7b3769dae522a338cbe6f2f3c5b202d75ff866830e96801c326a8823bf \
 *     -X security.protocol=ssl \
 *     -X ssl.certificate.location=client.crt -X ssl.key.location=client.key
 *
 * Get the thumbprint of a root CA certificate:
 *   openssl x509 -in root-ca.crt -noout -fingerprint -sha256
 *
 * Build against an installed librdkafka (uses its OpenSSL):
 *   cc root_ca_thumbprint.c -o root_ca_thumbprint \
 *      $(pkg-config --cflags --libs rdkafka libcrypto)
 *
 * See README.md in this folder.
 */

#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <openssl/evp.h>
#include <openssl/x509_vfy.h>

#include <librdkafka/rdkafka.h>

#define MAX_PINS 8

/** Pinned root CA thumbprints (32 bytes each). */
static unsigned char pins[MAX_PINS][32];
static int pin_cnt;

/**
 * Per-thread state of the chain being verified. librdkafka verifies a broker
 * connection's certificate chain on that broker's own thread, one certificate
 * after the other (root first, depth decreasing to 0 for the leaf).
 */
static __thread int chain_last_depth = -1; /* -1: no chain in progress */
static __thread int chain_root_ok;

static volatile sig_atomic_t run = 1;

static void stop(int sig) {
        run = 0;
}

/**
 * @brief Parse a thumbprint: 64 hex digits, colons / spaces ignored, any case.
 * @returns 0 on success, -1 on error.
 */
static int parse_thumbprint(const char *s, unsigned char *out) {
        int n = 0, hi = -1;
        for (; *s; s++) {
                int v;
                if (*s == ':' || *s == ' ')
                        continue;
                if (!isxdigit((unsigned char)*s))
                        return -1;
                v = isdigit((unsigned char)*s)
                        ? *s - '0'
                        : tolower((unsigned char)*s) - 'a' + 10;
                if (hi < 0) {
                        hi = v;
                } else {
                        if (n == 32)
                                return -1;
                        out[n++] = (unsigned char)(hi << 4 | v);
                        hi       = -1;
                }
        }
        return n == 32 && hi < 0 ? 0 : -1;
}

static void hex(const unsigned char *in, size_t len, char *out) {
        size_t i;
        for (i = 0; i < len; i++)
                sprintf(out + i * 2, "%02x", in[i]);
}

/**
 * @brief The ssl.certificate.verify_cb: pin the root CA by thumbprint.
 */
static int verify_cb(rd_kafka_t *rk,
                     const char *broker_name,
                     int32_t broker_id,
                     int *x509_error,
                     int depth,
                     const char *buf,
                     size_t size,
                     char *errstr,
                     size_t errstr_size,
                     void *opaque) {
        int top = chain_last_depth < 0 || depth >= chain_last_depth;

        if (top) /* First certificate of a new chain: the root. */
                chain_root_ok = 0;
        chain_last_depth = depth;

        if (top) {
                unsigned char digest[32];
                unsigned int dlen = 0;
                char digest_hex[65];
                int i;

                if (!EVP_Digest(buf, size, digest, &dlen, EVP_sha256(),
                                NULL) ||
                    dlen != 32) {
                        snprintf(errstr, errstr_size, "SHA-256 failed");
                        chain_last_depth = -1;
                        return 0;
                }
                hex(digest, 32, digest_hex);

                for (i = 0; i < pin_cnt; i++) {
                        if (!memcmp(digest, pins[i], 32))
                                break;
                }
                if (i == pin_cnt) {
                        snprintf(errstr, errstr_size,
                                 "%s: certificate at the top of the chain "
                                 "(depth %d, sha256 %s) is not a pinned root "
                                 "CA",
                                 broker_name, depth, digest_hex);
                        *x509_error      = X509_V_ERR_CERT_UNTRUSTED;
                        chain_last_depth = -1;
                        return 0;
                }
                /* Accept the pinned root. Only a self-signed root (the
                 * error OpenSSL reports for an untrusted root) or a root
                 * that OpenSSL already trusts (error 0) may be pinned:
                 * any other error, e.g. an expired root, stays fatal. */
                if (*x509_error != X509_V_OK &&
                    *x509_error != X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN &&
                    *x509_error != X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT) {
                        snprintf(errstr, errstr_size,
                                 "%s: pinned root CA rejected by OpenSSL: %s",
                                 broker_name,
                                 X509_verify_cert_error_string(*x509_error));
                        chain_last_depth = -1;
                        return 0;
                }
                fprintf(stderr,
                        "%% %s: root CA (depth %d) matches pinned thumbprint "
                        "%s\n",
                        broker_name, depth, digest_hex);
                *x509_error   = X509_V_OK;
                chain_root_ok = 1;
                if (depth == 0)
                        chain_last_depth = -1;
                /* A chain of only a self-signed root is not a broker
                 * certificate: refuse it. */
                if (depth == 0) {
                        snprintf(errstr, errstr_size,
                                 "%s: broker sent only a root CA "
                                 "certificate",
                                 broker_name);
                        return 0;
                }
                return 1;
        }

        /* Intermediate CA(s) and the leaf: OpenSSL's verdict, made with the
         * keys of the certificates above (ultimately the pinned root). */
        if (*x509_error != X509_V_OK) {
                snprintf(errstr, errstr_size,
                         "%s: certificate at depth %d failed: %s",
                         broker_name, depth,
                         X509_verify_cert_error_string(*x509_error));
                chain_last_depth = -1;
                return 0;
        }
        if (depth == 0) { /* Leaf: the end of this chain. */
                chain_last_depth = -1;
                if (!chain_root_ok) {
                        snprintf(errstr, errstr_size,
                                 "%s: no pinned root CA in this chain",
                                 broker_name);
                        return 0;
                }
                fprintf(stderr,
                        "%% %s: broker certificate chain verified from the "
                        "pinned root\n",
                        broker_name);
        }
        return 1;
}

static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
        int *delivered = (int *)opaque;
        if (rkmessage->err) {
                fprintf(stderr, "%% Delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
                *delivered = -1;
        } else {
                fprintf(stderr,
                        "%% Message delivered to %s [%d] at offset %lld\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        (int)rkmessage->partition,
                        (long long)rkmessage->offset);
                *delivered = 1;
        }
}

static void usage(const char *prog) {
        fprintf(stderr,
                "Usage: %s -b <brokers> -t <topic> -s <sha256>[,<sha256>...] "
                "[-X <property>=<value> ...] [-m <message>]\n",
                prog);
        exit(1);
}

int main(int argc, char **argv) {
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        char errstr[512];
        const char *brokers = NULL, *topic = NULL, *pinlist = NULL;
        const char *message = "hello from root_ca_thumbprint";
        int delivered       = 0;
        int opt;
        rd_kafka_resp_err_t err;

        conf = rd_kafka_conf_new();

        while ((opt = getopt(argc, argv, "b:t:s:X:m:")) != -1) {
                switch (opt) {
                case 'b':
                        brokers = optarg;
                        break;
                case 't':
                        topic = optarg;
                        break;
                case 's':
                        pinlist = optarg;
                        break;
                case 'm':
                        message = optarg;
                        break;
                case 'X': {
                        char *name = strdup(optarg), *val = strchr(name, '=');
                        if (!val)
                                usage(argv[0]);
                        *val++ = '\0';
                        if (rd_kafka_conf_set(conf, name, val, errstr,
                                              sizeof(errstr)) !=
                            RD_KAFKA_CONF_OK) {
                                fprintf(stderr, "%% %s\n", errstr);
                                return 1;
                        }
                        free(name);
                        break;
                }
                default:
                        usage(argv[0]);
                }
        }
        if (!brokers || !topic || !pinlist)
                usage(argv[0]);

        /* Pinned thumbprints. */
        {
                char *list = strdup(pinlist), *save = NULL, *tok;
                for (tok = strtok_r(list, ",", &save); tok;
                     tok = strtok_r(NULL, ",", &save)) {
                        if (pin_cnt == MAX_PINS ||
                            parse_thumbprint(tok, pins[pin_cnt]) == -1) {
                                fprintf(stderr,
                                        "%% Invalid thumbprint '%s': expected "
                                        "64 hex digits (SHA-256)\n",
                                        tok);
                                return 1;
                        }
                        pin_cnt++;
                }
                free(list);
        }

        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK ||
            rd_kafka_conf_set_ssl_cert_verify_cb(conf, verify_cb) !=
                RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%% %s\n", errstr);
                return 1;
        }
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
        rd_kafka_conf_set_opaque(conf, &delivered);

        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create producer: %s\n", errstr);
                return 1;
        }
        signal(SIGINT, stop);

        err = rd_kafka_producev(rk, RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_VALUE((void *)message,
                                                 strlen(message)),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                                RD_KAFKA_V_END);
        if (err) {
                fprintf(stderr, "%% Produce failed: %s\n",
                        rd_kafka_err2str(err));
                rd_kafka_destroy(rk);
                return 1;
        }

        /* Wait up to message.timeout.ms (set it with -X) for delivery. */
        while (run && delivered == 0)
                rd_kafka_poll(rk, 100);

        rd_kafka_flush(rk, 1000);
        rd_kafka_destroy(rk);
        return delivered == 1 ? 0 : 2;
}
