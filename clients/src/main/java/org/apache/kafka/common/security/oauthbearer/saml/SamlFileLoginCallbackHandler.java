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
package org.apache.kafka.common.security.oauthbearer.saml;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
 * may contain wrapped base64. The client does not parse the SAML token, so it cannot know the real expiry: it treats
 * the token as valid for {@code samlTokenLifetimeMs} (default 1 hour) and refreshes before then (see
 * {@code sasl.login.refresh.*}). Keep this at or below the real token lifetime.
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

    /** Principal name reported on the client side; the broker uses the subject from the validated token. */
    static final String CLIENT_SIDE_PRINCIPAL = "saml-token-holder";

    private final Time time;
    private Path tokenFile;
    private long tokenLifetimeMs = DEFAULT_TOKEN_LIFETIME_MS;

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
                    now + tokenLifetimeMs, now));
            } else {
                // e.g. SaslExtensionsCallback: no extensions are sent
                throw new UnsupportedCallbackException(callback);
            }
        }
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
