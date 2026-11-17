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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.authenticator.SuperUsersValidationIssue.Severity;
import org.apache.kafka.test.TestSslUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultSuperUsersValidatorTest {

    @TempDir
    Path dir;

    /** The principal Kafka builds for the test broker certificate, e.g. User:O=LocalDev,CN=broker1. */
    private String ownPrincipal;
    private String keystore;

    @BeforeEach
    public void setUp() throws Exception {
        KeyPair pair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate cert = TestSslUtils.generateCertificate("O=LocalDev,CN=broker1", pair, 30, "SHA256withRSA");
        keystore = dir.resolve("broker1.jks").toString();
        TestSslUtils.createKeyStore(keystore, new Password("secret"), new Password("secret"), "broker1",
            pair.getPrivate(), cert);
        ownPrincipal = "User:" + cert.getSubjectX500Principal().getName();
    }

    /** Broker settings like ours: inter-broker SSL with client certificates. */
    private Map<String, Object> sslBrokerConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("security.inter.broker.protocol", "SSL");
        configs.put("ssl.client.auth", "required");
        configs.put("ssl.keystore.location", keystore);
        configs.put("ssl.keystore.type", "JKS");
        configs.put("ssl.keystore.password", "secret");
        return configs;
    }

    private static List<SuperUsersValidationIssue> validate(Map<String, Object> configs, String... entries) {
        DefaultSuperUsersValidator validator = new DefaultSuperUsersValidator();
        validator.configure(configs);
        return validator.validate(Arrays.asList(entries));
    }

    private static List<SuperUsersValidationIssue> of(List<SuperUsersValidationIssue> issues, Severity severity) {
        return issues.stream().filter(i -> i.severity() == severity).collect(Collectors.toList());
    }

    @Test
    public void testValidConfigHasNoErrorsOrWarnings() {
        List<SuperUsersValidationIssue> issues = validate(sslBrokerConfigs(), ownPrincipal, "User:alice@example.com",
            "User:O=LocalDev,CN=admin-client");
        assertEquals(Collections.emptyList(), issues);
    }

    /** The broker's own principal with its RDNs reversed, i.e. the same DN in the wrong order. */
    private String wrongOrder() {
        List<String> rdns = Arrays.asList(ownPrincipal.substring("User:".length()).split(","));
        Collections.reverse(rdns);
        return "User:" + String.join(",", rdns);
    }

    @Test
    public void testOwnCertificateDnInWrongOrderIsError() {
        String wrong = wrongOrder();
        assertFalse(wrong.equals(ownPrincipal));
        List<SuperUsersValidationIssue> errors = of(validate(sslBrokerConfigs(), wrong), Severity.ERROR);

        assertEquals(1, errors.size());
        assertEquals(wrong, errors.get(0).entry().get());
        assertEquals(ownPrincipal, errors.get(0).suggestion().get());
        assertTrue(errors.get(0).message().contains("different order"));
    }

    @Test
    public void testOwnCertificateMissingIsWarning() {
        List<SuperUsersValidationIssue> issues = validate(sslBrokerConfigs(), "User:alice");
        assertTrue(of(issues, Severity.ERROR).isEmpty());
        List<SuperUsersValidationIssue> warnings = of(issues, Severity.WARNING);
        assertEquals(1, warnings.size());
        assertEquals(ownPrincipal, warnings.get(0).suggestion().get());
    }

    @Test
    public void testOwnCertificateFromListenerPrefixedKeystore() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("inter.broker.listener.name", "INTERNAL");
        configs.put("listener.security.protocol.map", "INTERNAL:SSL,EXTERNAL:SASL_SSL");
        configs.put("listener.name.internal.ssl.client.auth", "required");
        configs.put("listener.name.internal.ssl.keystore.location", keystore);
        configs.put("listener.name.internal.ssl.keystore.type", "JKS");
        configs.put("listener.name.internal.ssl.keystore.password", new Password("secret"));

        List<SuperUsersValidationIssue> errors = of(validate(configs, wrongOrder()), Severity.ERROR);
        assertEquals(1, errors.size());
        assertEquals(ownPrincipal, errors.get(0).suggestion().get());
    }

    @Test
    public void testDnNotInRfc2253FormIsError() {
        List<SuperUsersValidationIssue> errors = of(validate(Collections.emptyMap(),
            "User:CN=a, O=b", "User:cn=c,o=d"), Severity.ERROR);
        assertEquals(2, errors.size());
        assertEquals("User:CN=a,O=b", errors.get(0).suggestion().get());
        assertEquals("User:CN=c,O=d", errors.get(1).suggestion().get());
    }

    @Test
    public void testMappingRulesMakeDnUnmatchable() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("ssl.principal.mapping.rules", "RULE:^.*CN=([^,]+).*$/$1/,DEFAULT");

        List<SuperUsersValidationIssue> errors = of(validate(configs, "User:O=LocalDev,CN=broker2", "User:broker3"),
            Severity.ERROR);
        assertEquals(1, errors.size());
        assertEquals("User:broker2", errors.get(0).suggestion().get());
    }

    @Test
    public void testInvalidMappingRulesIsError() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("ssl.principal.mapping.rules", "RULE:^(.*$/$1/");
        assertFalse(of(validate(configs, "User:alice"), Severity.ERROR).isEmpty());
    }

    @Test
    public void testMalformedEntriesAreErrors() {
        List<SuperUsersValidationIssue> errors = of(validate(Collections.emptyMap(), "", "alice", "User:"), Severity.ERROR);
        assertEquals(3, errors.size());
        assertEquals("User:alice", errors.get(1).suggestion().get());
    }

    @Test
    public void testUnknownTypeAndClaimAreWarnings() {
        List<SuperUsersValidationIssue> issues = validate(Collections.emptyMap(), "Uesr:alice", "Claim:kafka-admins");
        assertTrue(of(issues, Severity.ERROR).isEmpty());
        List<SuperUsersValidationIssue> warnings = of(issues, Severity.WARNING);
        assertEquals(2, warnings.size());
        assertTrue(warnings.get(0).message().contains("Unknown principal type 'Uesr'"));
        assertTrue(warnings.get(1).message().contains("full access"));
    }

    @Test
    public void testConfiguredPrincipalTypes() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(DefaultSuperUsersValidator.PRINCIPAL_TYPES_CONFIG, "User,Group");
        List<SuperUsersValidationIssue> issues = validate(configs, "Group:admins");
        assertTrue(of(issues, Severity.ERROR).isEmpty());
        assertTrue(of(issues, Severity.WARNING).isEmpty());
        // Claim is not in the configured types: unknown-type warning plus the claim warning
        assertEquals(2, of(validate(configs, "Claim:x"), Severity.WARNING).size());
    }

    @Test
    public void testDuplicateIsInfo() {
        List<SuperUsersValidationIssue> issues = validate(Collections.emptyMap(), "User:alice", "User:alice");
        List<SuperUsersValidationIssue> duplicates = issues.stream()
            .filter(i -> i.message().startsWith("Duplicate")).collect(Collectors.toList());
        assertEquals(1, duplicates.size());
        assertEquals(Severity.INFO, duplicates.get(0).severity());
        assertTrue(of(issues, Severity.ERROR).isEmpty());
        assertTrue(of(issues, Severity.WARNING).isEmpty());
    }

    @Test
    public void testOwnCertificateCheckSkippedWithoutSslClientAuth() {
        Map<String, Object> plaintext = new HashMap<>();
        plaintext.put("security.inter.broker.protocol", "PLAINTEXT");
        List<SuperUsersValidationIssue> issues = validate(plaintext, "User:alice");
        assertEquals(1, issues.size());
        assertEquals(Severity.INFO, issues.get(0).severity());
        assertTrue(issues.get(0).message().contains("skipped"));

        Map<String, Object> noClientAuth = sslBrokerConfigs();
        noClientAuth.put("ssl.client.auth", "none");
        assertEquals(Severity.INFO, validate(noClientAuth, "User:alice").get(0).severity());
    }

    @Test
    public void testUnreadableKeystoreIsWarning() {
        Map<String, Object> configs = sslBrokerConfigs();
        configs.put("ssl.keystore.password", "wrong");
        List<SuperUsersValidationIssue> issues = validate(configs, "User:alice");
        assertTrue(of(issues, Severity.ERROR).isEmpty());
        assertEquals(1, of(issues, Severity.WARNING).size());
    }

    @Test
    public void testDnHelpers() {
        assertTrue(DefaultSuperUsersValidator.sameRdnsInAnyOrder("CN=a,O=b", "O=b,CN=a"));
        assertFalse(DefaultSuperUsersValidator.sameRdnsInAnyOrder("CN=a,O=b", "CN=a,O=b"));
        assertFalse(DefaultSuperUsersValidator.sameRdnsInAnyOrder("CN=a,O=b", "O=c,CN=a"));
        assertFalse(DefaultSuperUsersValidator.sameRdnsInAnyOrder("alice", "alice"));
        assertTrue(DefaultSuperUsersValidator.parseDn("alice@example.com") == null);
        assertTrue(DefaultSuperUsersValidator.parseDn("O=LocalDev,CN=broker1") != null);
    }

    @Test
    public void testIssueToString() {
        SuperUsersValidationIssue issue = new SuperUsersValidationIssue(Severity.ERROR, "User:x", "Bad.", "User:y");
        assertEquals("super.users entry 'User:x': Bad. Use: User:y", issue.toString());
    }
}
