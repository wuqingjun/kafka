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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServer;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SamlOAuthBearerValidatorCallbackHandlerTest {

    private final MockTime time = new MockTime(0, StubSamlTokenValidator.NOW_MS, 0);

    @BeforeEach
    public void setUp() {
        StubSamlTokenValidator.reset();
    }

    private SamlOAuthBearerValidatorCallbackHandler handler(Map<String, Object> extraConfigs) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SamlTokenValidator.VALIDATOR_CLASS_CONFIG, StubSamlTokenValidator.class.getName());
        configs.putAll(extraConfigs);
        SamlOAuthBearerValidatorCallbackHandler handler = new SamlOAuthBearerValidatorCallbackHandler(time);
        handler.configure(configs, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, Collections.emptyList());
        return handler;
    }

    private OAuthBearerValidatorCallback validate(SamlOAuthBearerValidatorCallbackHandler handler, String token)
            throws UnsupportedCallbackException {
        OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(token);
        handler.handle(new Callback[] {callback});
        return callback;
    }

    @Test
    public void testValidTokenBecomesOAuthBearerToken() throws Exception {
        OAuthBearerValidatorCallback callback = validate(handler(Collections.emptyMap()),
            StubSamlTokenValidator.VALID_TOKEN);

        OAuthBearerToken token = callback.token();
        assertInstanceOf(SamlOAuthBearerToken.class, token);
        assertEquals(StubSamlTokenValidator.VALID_TOKEN, token.value());
        assertEquals("alice@example.com", token.principalName());
        assertEquals(new LinkedHashSet<>(Arrays.asList("kafka-readers", "payments-team")), token.scope());
        assertEquals(StubSamlTokenValidator.EXPIRES_AT_MS, token.lifetimeMs());
        assertEquals(StubSamlTokenValidator.NOW_MS, token.startTimeMs());
        assertNull(callback.errorStatus());
    }

    @Test
    public void testInvalidTokenSetsError() throws Exception {
        OAuthBearerValidatorCallback callback = validate(handler(Collections.emptyMap()), "not-a-known-token");
        assertNull(callback.token());
        assertEquals("invalid_token", callback.errorStatus());
    }

    @Test
    public void testValidatorRuntimeExceptionSetsError() throws Exception {
        OAuthBearerValidatorCallback callback = validate(handler(Collections.emptyMap()),
            StubSamlTokenValidator.RUNTIME_ERROR_TOKEN);
        assertNull(callback.token());
        assertEquals("invalid_token", callback.errorStatus());
    }

    @Test
    public void testValidatorNullResultSetsError() throws Exception {
        OAuthBearerValidatorCallback callback = validate(handler(Collections.emptyMap()),
            StubSamlTokenValidator.NULL_RESULT_TOKEN);
        assertNull(callback.token());
        assertEquals("invalid_token", callback.errorStatus());
    }

    @Test
    public void testExpiredResultRejected() throws Exception {
        OAuthBearerValidatorCallback callback = validate(handler(Collections.emptyMap()),
            StubSamlTokenValidator.EXPIRED_TOKEN);
        assertNull(callback.token());
        assertEquals("invalid_token", callback.errorStatus());
    }

    @Test
    public void testNoExpiryUsesDefaultLifetime() throws Exception {
        OAuthBearerValidatorCallback callback = validate(handler(Collections.emptyMap()),
            StubSamlTokenValidator.NO_EXPIRY_TOKEN);
        assertEquals(StubSamlTokenValidator.NOW_MS + SamlOAuthBearerValidatorCallbackHandler.DEFAULT_LIFETIME_MS,
            callback.token().lifetimeMs());
    }

    @Test
    public void testConfiguredDefaultLifetime() throws Exception {
        Map<String, Object> extra = Collections.singletonMap(
            SamlOAuthBearerValidatorCallbackHandler.DEFAULT_LIFETIME_MS_CONFIG, "60000");
        OAuthBearerValidatorCallback callback = validate(handler(extra), StubSamlTokenValidator.NO_EXPIRY_TOKEN);
        assertEquals(StubSamlTokenValidator.NOW_MS + 60000, callback.token().lifetimeMs());
    }

    @Test
    public void testValidatorReceivesPrefixedConfigsWithPrefixRemoved() {
        Map<String, Object> extra = new HashMap<>();
        extra.put("saml.token.validator.audience", "kafka");
        extra.put("saml.token.validator.claim.attribute", "groups");
        extra.put("unrelated.setting", "x");
        handler(extra);

        Map<String, ?> received = StubSamlTokenValidator.lastConfigs;
        assertEquals("kafka", received.get("audience"));
        assertEquals("groups", received.get("claim.attribute"));
        assertEquals(StubSamlTokenValidator.class.getName(), received.get("class"));
        assertFalse(received.containsKey("unrelated.setting"));
        assertFalse(received.containsKey("saml.token.validator.audience"));
    }

    @Test
    public void testConfigErrors() {
        SamlOAuthBearerValidatorCallbackHandler h = new SamlOAuthBearerValidatorCallbackHandler(time);
        assertThrows(ConfigException.class,
            () -> h.configure(Collections.emptyMap(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, Collections.emptyList()));
        assertThrows(ConfigException.class, () -> h.configure(
            Collections.singletonMap(SamlTokenValidator.VALIDATOR_CLASS_CONFIG, "com.example.DoesNotExist"),
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, Collections.emptyList()));
        assertThrows(IllegalArgumentException.class, () -> h.configure(
            Collections.singletonMap(SamlTokenValidator.VALIDATOR_CLASS_CONFIG, StubSamlTokenValidator.class.getName()),
            "PLAIN", Collections.emptyList()));
        assertThrows(ConfigException.class, () -> handler(Collections.singletonMap(
            SamlOAuthBearerValidatorCallbackHandler.DEFAULT_LIFETIME_MS_CONFIG, "soon")));
        assertThrows(ConfigException.class, () -> handler(Collections.singletonMap(
            SamlOAuthBearerValidatorCallbackHandler.DEFAULT_LIFETIME_MS_CONFIG, "0")));
    }

    @Test
    public void testNotConfigured() {
        SamlOAuthBearerValidatorCallbackHandler h = new SamlOAuthBearerValidatorCallbackHandler(time);
        assertThrows(IllegalStateException.class,
            () -> h.handle(new Callback[] {new OAuthBearerValidatorCallback(StubSamlTokenValidator.VALID_TOKEN)}));
    }

    @Test
    public void testExtensionsIgnoredAndOtherCallbacksUnsupported() throws Exception {
        SamlOAuthBearerValidatorCallbackHandler h = handler(Collections.emptyMap());
        OAuthBearerToken token = validate(h, StubSamlTokenValidator.VALID_TOKEN).token();
        OAuthBearerExtensionsValidatorCallback extensions = new OAuthBearerExtensionsValidatorCallback(token,
            new SaslExtensions(Collections.singletonMap("traceId", "123")));
        h.handle(new Callback[] {extensions});
        assertTrue(extensions.validatedExtensions().isEmpty());
        assertTrue(extensions.invalidExtensions().isEmpty());

        assertThrows(UnsupportedCallbackException.class, () -> h.handle(new Callback[] {new SaslExtensionsCallback()}));
    }

    @Test
    public void testCloseClosesValidator() {
        SamlOAuthBearerValidatorCallbackHandler h = handler(Collections.emptyMap());
        h.close();
        assertTrue(StubSamlTokenValidator.closed);
    }

    /** End to end through Kafka's real OAUTHBEARER SASL server, with a token using + / and = padding. */
    @Test
    public void testSaslServerAcceptsValidSamlToken() throws Exception {
        OAuthBearerSaslServer server = new OAuthBearerSaslServer(handler(Collections.emptyMap()));
        byte[] clientFirstMessage = new OAuthBearerClientInitialResponse(StubSamlTokenValidator.VALID_TOKEN,
            SaslExtensions.empty()).toBytes();

        byte[] challenge = server.evaluateResponse(clientFirstMessage);

        assertArrayEquals(new byte[0], challenge);
        assertTrue(server.isComplete());
        assertEquals("alice@example.com", server.getAuthorizationID());
        OAuthBearerToken token = (OAuthBearerToken) server.getNegotiatedProperty("OAUTHBEARER.token");
        assertEquals(new LinkedHashSet<>(Arrays.asList("kafka-readers", "payments-team")), token.scope());
    }

    @Test
    public void testSaslServerRejectsInvalidSamlToken() throws Exception {
        OAuthBearerSaslServer server = new OAuthBearerSaslServer(handler(Collections.emptyMap()));
        byte[] clientFirstMessage = new OAuthBearerClientInitialResponse("dW5rbm93bg==",
            SaslExtensions.empty()).toBytes();

        byte[] challenge = server.evaluateResponse(clientFirstMessage);

        assertFalse(server.isComplete());
        assertTrue(new String(challenge, StandardCharsets.UTF_8).contains("invalid_token"));
    }
}
