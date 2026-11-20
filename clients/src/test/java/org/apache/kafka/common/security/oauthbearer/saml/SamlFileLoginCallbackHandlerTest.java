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

import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SamlFileLoginCallbackHandlerTest {

    private static final long NOW = 1_800_000_000_000L;

    @TempDir
    Path dir;

    private final MockTime time = new MockTime(0, NOW, 0);

    private static List<AppConfigurationEntry> jaas(Map<String, String> options) {
        return Collections.singletonList(new AppConfigurationEntry(OAuthBearerLoginModule.class.getName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options));
    }

    private SamlFileLoginCallbackHandler handler(Path file, String lifetimeMs) {
        Map<String, String> options = new HashMap<>();
        options.put(SamlFileLoginCallbackHandler.TOKEN_FILE_OPTION, file.toString());
        if (lifetimeMs != null)
            options.put(SamlFileLoginCallbackHandler.TOKEN_LIFETIME_MS_OPTION, lifetimeMs);
        SamlFileLoginCallbackHandler handler = new SamlFileLoginCallbackHandler(time);
        handler.configure(Collections.emptyMap(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaas(options));
        return handler;
    }

    private SamlFileLoginCallbackHandler handlerWith(Path file, Map<String, String> extra) {
        Map<String, String> options = new HashMap<>(extra);
        options.put(SamlFileLoginCallbackHandler.TOKEN_FILE_OPTION, file.toString());
        SamlFileLoginCallbackHandler handler = new SamlFileLoginCallbackHandler(time);
        handler.configure(Collections.emptyMap(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaas(options));
        return handler;
    }

    /** A minimal SAML-like assertion: NotOnOrAfter on the conditions and on the subject confirmation. */
    private static String samlToken(long conditionsNotOnOrAfter, long subjectNotOnOrAfter) {
        String xml = "<saml:Assertion xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\">"
            + "<saml:Subject><saml:SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\">"
            + "<saml:SubjectConfirmationData NotOnOrAfter=\"" + Instant.ofEpochMilli(subjectNotOnOrAfter) + "\"/>"
            + "</saml:SubjectConfirmation></saml:Subject>"
            + "<saml:Conditions NotBefore=\"" + Instant.ofEpochMilli(NOW - 60_000) + "\" NotOnOrAfter=\""
            + Instant.ofEpochMilli(conditionsNotOnOrAfter) + "\"/></saml:Assertion>";
        return Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8));
    }

    private long lifetimeFor(String token, Map<String, String> options) throws Exception {
        Path file = dir.resolve("token.b64");
        Files.write(file, token.getBytes(StandardCharsets.UTF_8));
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handlerWith(file, options).handle(new Callback[] {callback});
        return callback.token().lifetimeMs();
    }

    @Test
    public void testUsesTheTokensOwnExpiryEarliestNotOnOrAfter() throws Exception {
        long thirty = NOW + 30 * 60_000L;
        long forty = NOW + 40 * 60_000L;
        assertEquals(thirty, lifetimeFor(samlToken(thirty, forty), Collections.emptyMap()));
        assertEquals(thirty, lifetimeFor(samlToken(forty, thirty), Collections.emptyMap()));
    }

    @Test
    public void testConfiguredLifetimeCapsTheTokensExpiry() throws Exception {
        long thirty = NOW + 30 * 60_000L;
        String token = samlToken(thirty, thirty);
        assertEquals(NOW + 600_000L, lifetimeFor(token,
            Collections.singletonMap(SamlFileLoginCallbackHandler.TOKEN_LIFETIME_MS_OPTION, "600000")));
        assertEquals(thirty, lifetimeFor(token,
            Collections.singletonMap(SamlFileLoginCallbackHandler.TOKEN_LIFETIME_MS_OPTION, "7200000")));
    }

    @Test
    public void testExpiredTokenIsNotUsed() throws Exception {
        String token = samlToken(NOW - 1000, NOW + 600_000L);
        IOException e = assertThrows(IOException.class, () -> lifetimeFor(token, Collections.emptyMap()));
        assertTrue(e.getMessage().contains("expired at"), e.getMessage());
    }

    @Test
    public void testReadingTheExpiryCanBeDisabled() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(SamlFileLoginCallbackHandler.TOKEN_READ_EXPIRY_OPTION, "false");
        options.put(SamlFileLoginCallbackHandler.TOKEN_LIFETIME_MS_OPTION, "600000");
        assertEquals(NOW + 600_000L, lifetimeFor(samlToken(NOW - 1000, NOW - 1000), options));
        assertThrows(IllegalArgumentException.class, () -> handlerWith(dir.resolve("t"),
            Collections.singletonMap(SamlFileLoginCallbackHandler.TOKEN_READ_EXPIRY_OPTION, "maybe")));
    }

    @Test
    public void testEarliestNotOnOrAfterParsing() {
        String xml = "<a NotOnOrAfter='2026-11-19T10:00:00.123Z'/><b NotOnOrAfter = \"2026-11-19T09:30:00+00:00\"/>"
            + "<c NotOnOrAfter=\"not a date\"/>";
        String token = Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8));
        assertEquals(Instant.parse("2026-11-19T09:30:00Z").toEpochMilli(), SamlFileLoginCallbackHandler.earliestNotOnOrAfter(token));
        assertNull(SamlFileLoginCallbackHandler.earliestNotOnOrAfter("not base64 !!"));
        assertNull(SamlFileLoginCallbackHandler.earliestNotOnOrAfter(
            Base64.getEncoder().encodeToString("<x/>".getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testReadsTokenAndRemovesWhitespace() throws Exception {
        Path file = dir.resolve("token.b64");
        Files.write(file, "PHNhbWw+QUJD\n/xyz+w==\n".getBytes(StandardCharsets.UTF_8));
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();

        handler(file, null).handle(new Callback[] {callback});

        assertEquals("PHNhbWw+QUJD/xyz+w==", callback.token().value());
        assertEquals(NOW + SamlFileLoginCallbackHandler.DEFAULT_TOKEN_LIFETIME_MS, callback.token().lifetimeMs());
        assertEquals(NOW, callback.token().startTimeMs());
    }

    @Test
    public void testRereadsFileOnEveryLogin() throws Exception {
        Path file = dir.resolve("token.b64");
        SamlFileLoginCallbackHandler handler = handler(file, "60000");

        Files.write(file, "Zmlyc3Q=".getBytes(StandardCharsets.UTF_8));
        OAuthBearerTokenCallback first = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {first});

        Files.write(file, "c2Vjb25k".getBytes(StandardCharsets.UTF_8));
        OAuthBearerTokenCallback second = new OAuthBearerTokenCallback();
        handler.handle(new Callback[] {second});

        assertEquals("Zmlyc3Q=", first.token().value());
        assertEquals("c2Vjb25k", second.token().value());
        assertEquals(NOW + 60000, second.token().lifetimeMs());
    }

    @Test
    public void testEmptyOrMissingFile() throws Exception {
        Path empty = dir.resolve("empty");
        Files.write(empty, " \n".getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class,
            () -> handler(empty, null).handle(new Callback[] {new OAuthBearerTokenCallback()}));
        assertThrows(IOException.class,
            () -> handler(dir.resolve("missing"), null).handle(new Callback[] {new OAuthBearerTokenCallback()}));
    }

    @Test
    public void testConfigErrors() {
        SamlFileLoginCallbackHandler h = new SamlFileLoginCallbackHandler(time);
        assertThrows(IllegalArgumentException.class, () -> h.configure(Collections.emptyMap(),
            OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaas(Collections.emptyMap())));
        assertThrows(IllegalArgumentException.class, () -> h.configure(Collections.emptyMap(), "PLAIN",
            jaas(Collections.singletonMap(SamlFileLoginCallbackHandler.TOKEN_FILE_OPTION, "/tmp/x"))));
        assertThrows(IllegalArgumentException.class, () -> handler(dir.resolve("t"), "soon"));
        assertThrows(IllegalArgumentException.class, () -> handler(dir.resolve("t"), "-1"));
    }

    @Test
    public void testExtensionsCallbackUnsupported() {
        SamlFileLoginCallbackHandler h = handler(dir.resolve("t"), null);
        assertThrows(UnsupportedCallbackException.class, () -> h.handle(new Callback[] {new SaslExtensionsCallback()}));
    }
}
