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
package org.apache.kafka.common.security.oauthbearer.saml;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

/**
 * Client-side SASL/OAUTHBEARER login callback handler that reads a base64-encoded SAML token from a file.
 * <p>
 * A reference implementation for testing and for setups where another process (e.g. a sidecar) keeps a fresh token
 * in a file. Enterprises with their own token source write their own login callback handler.
 * <p>
 * The file is re-read on every login and token refresh. Whitespace (including line breaks) is removed, so the file
 * may contain wrapped base64.
 * <p>
 * <b>Token lifetime.</b> The handler reads the token's real expiry: the earliest {@code NotOnOrAfter} in the decoded
 * assertion (conditions, subject confirmation), so Kafka refreshes before the token actually expires (see
 * {@code sasl.login.refresh.*}) even if the file was written a while ago. This is a plain text scan for the attribute
 * — no XML parsing, no trust decision (validating the token is the broker's job). A token that has already expired is
 * not used: the login (or refresh) fails with a clear error and Kafka's refresh logic retries, picking up a newly
 * written file.
 * <ul>
 *   <li>{@code samlTokenLifetimeMs}: if set, an upper bound on the lifetime; used as the lifetime when the token has
 *       no readable {@code NotOnOrAfter} (default 1 hour).</li>
 *   <li>{@code samlTokenReadExpiry="false"}: don't read the token, always use {@code samlTokenLifetimeMs} (previous
 *       behavior; keep it at or below the real remaining token lifetime).</li>
 * </ul>
 *
 * <h3>Client configuration</h3>
 * <pre>
 * security.protocol=SASL_SSL
 * sasl.mechanism=OAUTHBEARER
 * sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.saml.SamlFileLoginCallbackHandler
 * sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
 *     samlTokenFile="/path/to/token.b64" samlTokenLifetimeMs="3600000";
 * </pre>
 */
public class SamlFileLoginCallbackHandler implements AuthenticateCallbackHandler {

    public static final String TOKEN_FILE_OPTION = "samlTokenFile";
    public static final String TOKEN_LIFETIME_MS_OPTION = "samlTokenLifetimeMs";
    public static final long DEFAULT_TOKEN_LIFETIME_MS = 60 * 60 * 1000L;
    public static final String TOKEN_READ_EXPIRY_OPTION = "samlTokenReadExpiry";
    private static final Pattern NOT_ON_OR_AFTER = Pattern.compile("NotOnOrAfter\\s*=\\s*[\"']([^\"']+)[\"']");
    private static final Logger log = LoggerFactory.getLogger(SamlFileLoginCallbackHandler.class);

    /** Principal name reported on the client side; the broker uses the subject from the validated token. */
    static final String CLIENT_SIDE_PRINCIPAL = "saml-token-holder";

    private final Time time;
    private Path tokenFile;
    private long tokenLifetimeMs = DEFAULT_TOKEN_LIFETIME_MS;
    private boolean lifetimeConfigured;
    private boolean readExpiry = true;
    private boolean warnedNoExpiry;

    public SamlFileLoginCallbackHandler() {
        this(Time.SYSTEM);
    }

    // Visible for testing
    SamlFileLoginCallbackHandler(Time time) {
        this.time = time;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
        if (jaasConfigEntries == null || jaasConfigEntries.size() != 1 || jaasConfigEntries.get(0) == null)
            throw new IllegalArgumentException("Must supply exactly 1 non-null JAAS mechanism configuration");
        Map<String, Object> options = (Map<String, Object>) jaasConfigEntries.get(0).getOptions();

        Object file = options.get(TOKEN_FILE_OPTION);
        if (file == null || Utils.isBlank(file.toString()))
            throw new IllegalArgumentException("Missing JAAS option " + TOKEN_FILE_OPTION);
        tokenFile = Paths.get(file.toString().trim());

        Object lifetime = options.get(TOKEN_LIFETIME_MS_OPTION);
        if (lifetime != null) {
            try {
                tokenLifetimeMs = Long.parseLong(lifetime.toString().trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("JAAS option " + TOKEN_LIFETIME_MS_OPTION + " must be a number", e);
            }
            if (tokenLifetimeMs <= 0)
                throw new IllegalArgumentException("JAAS option " + TOKEN_LIFETIME_MS_OPTION + " must be positive");
            lifetimeConfigured = true;
        }
        Object read = options.get(TOKEN_READ_EXPIRY_OPTION);
        if (read != null) {
            String value = read.toString().trim();
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false"))
                throw new IllegalArgumentException("JAAS option " + TOKEN_READ_EXPIRY_OPTION + " must be true or false");
            readExpiry = Boolean.parseBoolean(value);
        }
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (tokenFile == null)
            throw new IllegalStateException("Callback handler not configured");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                OAuthBearerTokenCallback tokenCallback = (OAuthBearerTokenCallback) callback;
                String token = readToken();
                long now = time.milliseconds();
                tokenCallback.token(new SamlOAuthBearerToken(token, CLIENT_SIDE_PRINCIPAL, Collections.emptySet(),
                    expiry(token, now), now));
            } else {
                // e.g. SaslExtensionsCallback: no extensions are sent
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    /**
     * @return when the token must no longer be used: its earliest NotOnOrAfter (capped by samlTokenLifetimeMs if set),
     *         or now + samlTokenLifetimeMs if the token has none (or reading it is disabled)
     * @throws IOException if the token has already expired
     */
    private long expiry(String token, long now) throws IOException {
        long configured = now + tokenLifetimeMs;
        if (!readExpiry)
            return configured;
        Long notOnOrAfter = earliestNotOnOrAfter(token);
        if (notOnOrAfter == null) {
            if (!warnedNoExpiry) {
                log.warn("SAML token in {} has no readable NotOnOrAfter; treating it as valid for {} ms ({})", tokenFile,
                    tokenLifetimeMs, TOKEN_LIFETIME_MS_OPTION);
                warnedNoExpiry = true;
            }
            return configured;
        }
        if (notOnOrAfter <= now)
            throw new IOException("SAML token in " + tokenFile + " expired at " + Instant.ofEpochMilli(notOnOrAfter)
                + "; not using it (waiting for a fresh token in the file)");
        return lifetimeConfigured ? Math.min(notOnOrAfter, configured) : notOnOrAfter;
    }

    /**
     * @return the earliest NotOnOrAfter in the decoded token (epoch ms), or null if there is none / it can't be read
     */
    static Long earliestNotOnOrAfter(String token) {
        String xml;
        try {
            xml = new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return null;
        }
        Long earliest = null;
        Matcher m = NOT_ON_OR_AFTER.matcher(xml);
        while (m.find()) {
            try {
                long t = OffsetDateTime.parse(m.group(1).trim()).toInstant().toEpochMilli();
                earliest = earliest == null ? t : Math.min(earliest, t);
            } catch (DateTimeParseException e) {
                // not a date: ignore this attribute
            }
        }
        return earliest;
    }

    private String readToken() throws IOException {
        String token = new String(Files.readAllBytes(tokenFile), StandardCharsets.UTF_8).replaceAll("\\s", "");
        if (token.isEmpty())
            throw new IOException("SAML token file " + tokenFile + " is empty");
        return token;
    }

    @Override
    public void close() {
    }
}
