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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

/**
 * Broker-side SASL/OAUTHBEARER callback handler that validates SAML tokens with a pluggable
 * {@link SamlTokenValidator}.
 * <p>
 * The client sends its SAML token base64-encoded as the OAUTHBEARER token. This handler passes the token to the
 * validator named by {@value SamlTokenValidator#VALIDATOR_CLASS_CONFIG} and, if valid, returns a
 * {@link SamlOAuthBearerToken} whose principal name is the token subject and whose scope is the token claims.
 * The default principal builder then creates the principal {@code User:<subject>}.
 *
 * <h3>Broker configuration</h3>
 * <pre>
 * listeners=SSL://host:9092,SASL_SSL://host:9192
 * sasl.enabled.mechanisms=OAUTHBEARER
 * listener.name.sasl_ssl.oauthbearer.sasl.jaas.config=\
 *     org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
 * listener.name.sasl_ssl.oauthbearer.sasl.server.callback.handler.class=\
 *     org.apache.kafka.common.security.oauthbearer.saml.SamlOAuthBearerValidatorCallbackHandler
 * saml.token.validator.class=com.example.MySamlTokenValidator
 * saml.token.validator.&lt;name&gt;=&lt;value&gt;          # passed to the validator, prefix removed
 * saml.token.default.lifetime.ms=3600000             # optional, for tokens without an expiry
 * connections.max.reauth.ms=3600000                  # optional: also cap sessions independently of token expiry
 * </pre>
 * <p>
 * Session lifetime: Kafka ends an OAUTHBEARER session when the token expires (the lifetime returned here), whether
 * or not {@code connections.max.reauth.ms} is set; Kafka clients (2.2+) re-authenticate on the open connection with a
 * fresh token before then (see {@link SamlFileLoginCallbackHandler}). {@code connections.max.reauth.ms} only shortens
 * sessions further.
 * The {@code saml.token.*} settings are read from the top level of the broker configuration (not listener-prefixed).
 * <p>
 * Invalid tokens: the validator's reason is logged at WARN on the broker; the client only receives
 * {@code invalid_token}. SASL extensions sent by the client are ignored.
 */
public class SamlOAuthBearerValidatorCallbackHandler implements AuthenticateCallbackHandler {

    /** Lifetime given to tokens whose validator result has no expiry. */
    public static final String DEFAULT_LIFETIME_MS_CONFIG = "saml.token.default.lifetime.ms";
    public static final long DEFAULT_LIFETIME_MS = 60 * 60 * 1000L;

    private static final Logger log = LoggerFactory.getLogger(SamlOAuthBearerValidatorCallbackHandler.class);

    private final Time time;
    private SamlTokenValidator validator;
    private long defaultLifetimeMs = DEFAULT_LIFETIME_MS;

    public SamlOAuthBearerValidatorCallbackHandler() {
        this(Time.SYSTEM);
    }

    // Visible for testing
    SamlOAuthBearerValidatorCallbackHandler(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism))
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);

        Object className = configs.get(SamlTokenValidator.VALIDATOR_CLASS_CONFIG);
        if (className == null || Utils.isBlank(className.toString()))
            throw new ConfigException("Missing required broker setting " + SamlTokenValidator.VALIDATOR_CLASS_CONFIG);

        Object lifetime = configs.get(DEFAULT_LIFETIME_MS_CONFIG);
        if (lifetime != null) {
            try {
                defaultLifetimeMs = Long.parseLong(lifetime.toString().trim());
            } catch (NumberFormatException e) {
                throw new ConfigException(DEFAULT_LIFETIME_MS_CONFIG, lifetime, "must be a number of milliseconds");
            }
            if (defaultLifetimeMs <= 0)
                throw new ConfigException(DEFAULT_LIFETIME_MS_CONFIG, lifetime, "must be positive");
        }

        Map<String, Object> validatorConfigs = new HashMap<>();
        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(SamlTokenValidator.VALIDATOR_CONFIG_PREFIX)
                && key.length() > SamlTokenValidator.VALIDATOR_CONFIG_PREFIX.length())
                validatorConfigs.put(key.substring(SamlTokenValidator.VALIDATOR_CONFIG_PREFIX.length()), entry.getValue());
        }

        SamlTokenValidator newValidator;
        try {
            newValidator = Utils.newInstance(className.toString().trim(), SamlTokenValidator.class);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(SamlTokenValidator.VALIDATOR_CLASS_CONFIG, className,
                "class not found on the broker classpath");
        }
        newValidator.configure(validatorConfigs);
        this.validator = newValidator;
        log.info("SAML token validation enabled with {}", newValidator.getClass().getName());
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        if (validator == null)
            throw new IllegalStateException("Callback handler not configured");
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerValidatorCallback) {
                handleToken((OAuthBearerValidatorCallback) callback);
            } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
                // SASL extensions are not used: leave them unvalidated, so they are ignored (not an error).
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleToken(OAuthBearerValidatorCallback callback) {
        String base64Token = callback.tokenValue();
        SamlTokenValidationResult result;
        try {
            result = validator.validate(base64Token);
        } catch (SamlTokenValidationException e) {
            log.warn("SAML token rejected: {}", e.getMessage());
            callback.error("invalid_token", null, null);
            return;
        } catch (RuntimeException e) {
            log.error("SAML token validator {} failed unexpectedly", validator.getClass().getName(), e);
            callback.error("invalid_token", null, null);
            return;
        }
        if (result == null) {
            log.error("SAML token validator {} returned null instead of a result or an exception",
                validator.getClass().getName());
            callback.error("invalid_token", null, null);
            return;
        }

        long now = time.milliseconds();
        long expiresAtMs = result.expiresAtMs().isPresent() ? result.expiresAtMs().getAsLong() : now + defaultLifetimeMs;
        if (expiresAtMs <= now) {
            log.warn("SAML token rejected: expired at {} (subject {})", expiresAtMs, result.subject());
            callback.error("invalid_token", null, null);
            return;
        }

        SamlOAuthBearerToken token = new SamlOAuthBearerToken(base64Token, result.subject(), result.claims(),
            expiresAtMs, now);
        log.debug("SAML token accepted: {}", token);
        callback.token(token);
    }

    @Override
    public void close() {
        if (validator != null) {
            Utils.closeQuietly(validator, "SAML token validator");
            validator = null;
        }
    }
}
