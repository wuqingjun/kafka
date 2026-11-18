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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestSslUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PKI used by the tests:
 * <pre>
 *   Root CA ── Intermediate CA ── server (CN=localhost)     broker side, sends the 3-level chain
 *   Team B CA ── teamb client
 *   Team C CA ── teamc client                               Team C CA is never trusted
 *   Team B CA ── expired client
 * </pre>
 */
public class OsCertStoreSslEngineFactoryTest {
    private static final String ALG = "SHA256withRSA";

    @TempDir
    Path tmp;

    private KeyPair rootKeys;
    private X509Certificate root;
    private KeyPair intermediateKeys;
    private X509Certificate intermediate;
    private KeyPair serverKeys;
    private X509Certificate server;
    private KeyPair teamBCaKeys;
    private X509Certificate teamBCa;
    private KeyPair teamCCaKeys;
    private X509Certificate teamCCa;

    @BeforeEach
    public void setUp() throws Exception {
        rootKeys = TestSslUtils.generateKeyPair("RSA");
        root = TestSslUtils.generateSignedCertificate("CN=Root CA,O=Test", rootKeys, 1, 30, null, null, ALG, true, false, false);
        intermediateKeys = TestSslUtils.generateKeyPair("RSA");
        intermediate = TestSslUtils.generateSignedCertificate("CN=Intermediate CA,O=Test", intermediateKeys, 1, 30,
            "CN=Root CA,O=Test", rootKeys, ALG, true, false, false);
        serverKeys = TestSslUtils.generateKeyPair("RSA");
        server = TestSslUtils.generateSignedCertificate("CN=localhost,O=Test", serverKeys, 1, 30,
            "CN=Intermediate CA,O=Test", intermediateKeys, ALG, false, true, true, new String[]{"localhost"});
        teamBCaKeys = TestSslUtils.generateKeyPair("RSA");
        teamBCa = TestSslUtils.generateSignedCertificate("CN=Team B CA,O=TeamB", teamBCaKeys, 1, 30, null, null, ALG, true, false, false);
        teamCCaKeys = TestSslUtils.generateKeyPair("RSA");
        teamCCa = TestSslUtils.generateSignedCertificate("CN=Team C CA,O=TeamC", teamCCaKeys, 1, 30, null, null, ALG, true, false, false);
    }

    // ---- helpers ------------------------------------------------------------------------------------------------

    private static String pem(Certificate... certificates) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (Certificate c : certificates)
            sb.append("-----BEGIN CERTIFICATE-----\n")
                .append(Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(c.getEncoded()))
                .append("\n-----END CERTIFICATE-----\n");
        return sb.toString();
    }

    private static String pemKey(KeyPair keys) {
        return "-----BEGIN PRIVATE KEY-----\n" +
            Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(keys.getPrivate().getEncoded()) +
            "\n-----END PRIVATE KEY-----\n";
    }

    private int stores;

    private Path dir(String name) throws Exception {
        Path dir = tmp.resolve(name + "-" + stores++);
        Files.createDirectories(dir);
        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        return dir;
    }

    private static void write(Path file, String content) throws Exception {
        Files.write(file, content.getBytes(StandardCharsets.US_ASCII));
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
    }

    /** A store: trust folder with the given CA files, key folder with the chain and key. */
    private Map<String, Object> store(String name, Map<String, String> trustFiles, String chainPem, String keyPem) throws Exception {
        Path trust = dir(name + "-trust");
        for (Map.Entry<String, String> e : trustFiles.entrySet())
            write(trust.resolve(e.getKey()), e.getValue());
        Map<String, Object> configs = new HashMap<>();
        configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3");
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, Arrays.asList("TLSv1.3", "TLSv1.2"));
        configs.put(OsCertStoreSslEngineFactory.TRUST_DIR_CONFIG, trust.toString());
        if (chainPem != null) {
            Path key = dir(name + "-key");
            write(key.resolve(DirectoryCertificateStore.CHAIN_FILE), chainPem);
            write(key.resolve(DirectoryCertificateStore.KEY_FILE), keyPem);
            configs.put(OsCertStoreSslEngineFactory.KEY_DIR_CONFIG, key.toString());
        }
        return configs;
    }

    /** Broker side: trusts Root CA (own chain) and Team B CA; sends leaf + intermediate + root; client auth required. */
    private Map<String, Object> brokerStore() throws Exception {
        Map<String, String> trust = new HashMap<>();
        trust.put("root-ca.pem", pem(root));
        trust.put("teamb-ca.crt", pem(teamBCa));
        Map<String, Object> configs = store("broker", trust, pem(server, intermediate, root), pemKey(serverKeys));
        configs.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
        return configs;
    }

    /** Client side: trusts the Root CA only (the intermediate comes from the broker); own certificate from a team CA. */
    private Map<String, Object> clientStore(String name, String dn, String caDn, KeyPair caKeys, X509Certificate ca,
                                            int daysBefore, int daysAfter) throws Exception {
        KeyPair keys = TestSslUtils.generateKeyPair("RSA");
        X509Certificate cert = TestSslUtils.generateSignedCertificate(dn, keys, daysBefore, daysAfter,
            caDn, caKeys, ALG, false, false, true);
        return store(name, java.util.Collections.singletonMap("root.pem", pem(root)), pem(cert, ca), pemKey(keys));
    }

    private static OsCertStoreSslEngineFactory factory(Map<String, Object> configs) {
        OsCertStoreSslEngineFactory factory = new OsCertStoreSslEngineFactory(new MockTime());
        factory.configure(configs);
        return factory;
    }

    private static SSLEngine lastClientEngine;

    /** Runs a TLS handshake in memory; returns the server engine (the client engine is in {@link #lastClientEngine}). */
    private static SSLEngine handshake(OsCertStoreSslEngineFactory client, OsCertStoreSslEngineFactory serverFactory) throws Exception {
        SSLEngine c = client.createClientSslEngine("localhost", 9092, "https");
        SSLEngine s = serverFactory.createServerSslEngine("localhost", 9092);
        lastClientEngine = c;
        int size = Math.max(c.getSession().getPacketBufferSize(), s.getSession().getPacketBufferSize());
        ByteBuffer cOut = ByteBuffer.allocate(size);
        ByteBuffer sOut = ByteBuffer.allocate(size);
        ByteBuffer cApp = ByteBuffer.allocate(c.getSession().getApplicationBufferSize());
        ByteBuffer sApp = ByteBuffer.allocate(s.getSession().getApplicationBufferSize());
        ByteBuffer empty = ByteBuffer.allocate(0);
        c.beginHandshake();
        s.beginHandshake();
        for (int i = 0; i < 1000; i++) {
            boolean cDone = done(c);
            boolean sDone = done(s);
            if (cDone && sDone && cOut.position() == 0 && sOut.position() == 0)
                return s;
            step(c, empty, cOut, sOut, cApp);
            step(s, empty, sOut, cOut, sApp);
        }
        throw new AssertionError("handshake did not finish");
    }

    private static boolean done(SSLEngine e) {
        SSLEngineResult.HandshakeStatus st = e.getHandshakeStatus();
        return st == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING || st == SSLEngineResult.HandshakeStatus.FINISHED;
    }

    /** One step for engine e: run tasks, wrap into own outbound buffer, unwrap from the peer's outbound buffer. */
    private static void step(SSLEngine e, ByteBuffer empty, ByteBuffer myOut, ByteBuffer peerOut, ByteBuffer app) throws SSLException {
        Runnable task;
        while ((task = e.getDelegatedTask()) != null)
            task.run();
        if (e.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP)
            e.wrap(empty, myOut);
        if (peerOut.position() > 0) {
            peerOut.flip();
            e.unwrap(peerOut, app);
            peerOut.compact();
            while ((task = e.getDelegatedTask()) != null)
                task.run();
        }
    }

    // ---- tests --------------------------------------------------------------------------------------------------

    @Test
    public void clientsFromDifferentCasAreAuthenticatedAndBrokerSendsFullChain() throws Exception {
        OsCertStoreSslEngineFactory broker = factory(brokerStore());
        OsCertStoreSslEngineFactory teamB = factory(clientStore("teamb", "CN=teamb-app,O=TeamB", "CN=Team B CA,O=TeamB", teamBCaKeys, teamBCa, 1, 30));

        SSLEngine serverEngine = handshake(teamB, broker);
        assertTrue(serverEngine.getSession().getPeerPrincipal().getName().contains("CN=teamb-app"));

        // The broker's key entry holds the full chain leaf -> intermediate -> root, which it sends in the handshake.
        assertArrayEquals(new Certificate[]{server, intermediate, root}, broker.keystore().getCertificateChain("kafka"));
    }

    @Test
    public void brokerToBrokerWithIntermediateChain() throws Exception {
        // Inter-broker: both sides use the broker store; the client side verifies the hostname "localhost".
        OsCertStoreSslEngineFactory b1 = factory(brokerStore());
        OsCertStoreSslEngineFactory b2 = factory(brokerStore());
        SSLEngine serverEngine = handshake(b1, b2);
        assertTrue(serverEngine.getSession().getPeerPrincipal().getName().contains("CN=localhost"));
        assertEquals(3, serverEngine.getSession().getPeerCertificates().length);
    }

    @Test
    public void clientFromUntrustedCaIsRejected() throws Exception {
        OsCertStoreSslEngineFactory broker = factory(brokerStore());
        OsCertStoreSslEngineFactory teamC = factory(clientStore("teamc", "CN=teamc-app,O=TeamC", "CN=Team C CA,O=TeamC", teamCCaKeys, teamCCa, 1, 30));
        assertThrows(SSLException.class, () -> handshake(teamC, broker));
    }

    @Test
    public void expiredClientCertificateIsRejectedEvenFromTrustedCa() throws Exception {
        OsCertStoreSslEngineFactory broker = factory(brokerStore());
        Map<String, Object> expiredConfigs = clientStore("expired", "CN=old-app,O=TeamB", "CN=Team B CA,O=TeamB", teamBCaKeys, teamBCa, 30, -1);
        // Our own factory refuses to load an expired chain; simulate a client that sends one anyway.
        KafkaException refused = assertThrows(KafkaException.class, () -> factory(expiredConfigs));
        assertTrue(refused.getMessage().contains("is not valid now"), refused.getMessage());
        expiredConfigs.put(OsCertStoreSslEngineFactory.REQUIRE_FULL_CHAIN_CONFIG, "false");
        OsCertStoreSslEngineFactory expired = factory(expiredConfigs);
        assertThrows(SSLException.class, () -> handshake(expired, broker));
    }

    @Test
    public void multiCaTrustManagerTriesEachCa() throws Exception {
        KeyPair keys = TestSslUtils.generateKeyPair("RSA");
        X509Certificate client = TestSslUtils.generateSignedCertificate("CN=a,O=TeamB", keys, 1, 30,
            "CN=Team B CA,O=TeamB", teamBCaKeys, ALG, false, false, true);
        MultiCaTrustManager tm = new MultiCaTrustManager(Arrays.asList(
            new OsCertificateStore.TrustedCa("root", root), new OsCertificateStore.TrustedCa("teamc", teamCCa),
            new OsCertificateStore.TrustedCa("teamb", teamBCa)), "PKIX");
        tm.checkClientTrusted(new X509Certificate[]{client}, "RSA");   // third CA validates it
        MultiCaTrustManager onlyOthers = new MultiCaTrustManager(Arrays.asList(
            new OsCertificateStore.TrustedCa("root", root), new OsCertificateStore.TrustedCa("teamc", teamCCa)), "PKIX");
        assertThrows(java.security.cert.CertificateException.class,
            () -> onlyOthers.checkClientTrusted(new X509Certificate[]{client}, "RSA"));
        // A chain through the intermediate validates against the root alone.
        onlyOthers.checkServerTrusted(new X509Certificate[]{server, intermediate}, "RSA");
        assertEquals(3, tm.getAcceptedIssuers().length);
    }

    @Test
    public void severalCertificatesInOneFileAreSeparateCas() throws Exception {
        Map<String, Object> configs = store("bundle", java.util.Collections.singletonMap("bundle.pem", pem(root, teamBCa, teamCCa)),
            null, null);
        OsCertStoreSslEngineFactory f = factory(configs);
        assertEquals(3, f.trustedCas().size());
        assertEquals(3, f.truststore().size());
    }

    @Test
    public void tooOpenPermissionsAreRefused() throws Exception {
        Map<String, Object> configs = brokerStore();
        Path key = Paths.get(configs.get(OsCertStoreSslEngineFactory.KEY_DIR_CONFIG).toString()).resolve("key.pem");
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-r--r--"));
        KafkaException e = assertThrows(KafkaException.class, () -> factory(configs));
        assertTrue(e.getMessage().contains("key.pem grants"), e.getMessage());

        Map<String, Object> configs2 = brokerStore();
        Path trust = Paths.get(configs2.get(OsCertStoreSslEngineFactory.TRUST_DIR_CONFIG).toString());
        Files.setPosixFilePermissions(trust, PosixFilePermissions.fromString("rwxr-x---"));
        assertThrows(KafkaException.class, () -> factory(configs2));

        // With the check off (e.g. for tests) the same store loads.
        configs2.put(OsCertStoreSslEngineFactory.PERMISSION_CHECK_CONFIG, "false");
        assertEquals(2, factory(configs2).trustedCas().size());
    }

    @Test
    public void symbolicLinksAndEncryptedKeysAreRefused() throws Exception {
        Map<String, Object> configs = brokerStore();
        Path trust = Paths.get(configs.get(OsCertStoreSslEngineFactory.TRUST_DIR_CONFIG).toString());
        Path outside = tmp.resolve("outside.pem");
        write(outside, pem(teamCCa));
        Files.createSymbolicLink(trust.resolve("link.pem"), outside);
        assertThrows(KafkaException.class, () -> factory(configs));

        Map<String, Object> configs2 = brokerStore();
        Path key = Paths.get(configs2.get(OsCertStoreSslEngineFactory.KEY_DIR_CONFIG).toString()).resolve("key.pem");
        write(key, "-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n");
        KafkaException e = assertThrows(KafkaException.class, () -> factory(configs2));
        assertTrue(e.getMessage().contains("encrypted keys are not supported"), e.getMessage());
    }

    @Test
    public void caAddedAndRemovedWithoutRestartAndBadStoreKeepsLastGood() throws Exception {
        Map<String, Object> configs = brokerStore();
        configs.put(OsCertStoreSslEngineFactory.RELOAD_CHECK_INTERVAL_MS_CONFIG, "1000");
        MockTime time = new MockTime();
        OsCertStoreSslEngineFactory broker = new OsCertStoreSslEngineFactory(time);
        broker.configure(configs);
        OsCertStoreSslEngineFactory teamC = factory(clientStore("teamc", "CN=teamc-app,O=TeamC", "CN=Team C CA,O=TeamC", teamCCaKeys, teamCCa, 1, 30));
        Path trust = Paths.get(configs.get(OsCertStoreSslEngineFactory.TRUST_DIR_CONFIG).toString());

        assertThrows(SSLException.class, () -> handshake(teamC, broker));

        // Add Team C's CA: picked up after the check interval, no restart.
        write(trust.resolve("teamc-ca.pem"), pem(teamCCa));
        assertFalse(broker.maybeReload(), "not before the interval");
        time.sleep(1001);
        assertTrue(broker.maybeReload());
        assertEquals(3, broker.trustedCas().size());
        assertTrue(handshake(teamC, broker).getSession().getPeerPrincipal().getName().contains("CN=teamc-app"));

        // A too-open file makes the store invalid: the last good content (with Team C) stays.
        Files.setPosixFilePermissions(trust.resolve("teamc-ca.pem"), PosixFilePermissions.fromString("rw-rw-rw-"));
        time.sleep(1001);
        assertFalse(broker.maybeReload());
        assertEquals(3, broker.trustedCas().size());

        // Remove Team C's CA: rejected again.
        Files.delete(trust.resolve("teamc-ca.pem"));
        time.sleep(1001);
        assertTrue(broker.maybeReload());
        assertEquals(2, broker.trustedCas().size());
        assertThrows(SSLException.class, () -> handshake(teamC, broker));
    }

    /** Broker store whose chain.pem has the given content. */
    private Map<String, Object> brokerStoreWithChain(String chainPem) throws Exception {
        Map<String, Object> configs = brokerStore();
        Path key = Paths.get(configs.get(OsCertStoreSslEngineFactory.KEY_DIR_CONFIG).toString());
        write(key.resolve(DirectoryCertificateStore.CHAIN_FILE), chainPem);
        return configs;
    }

    private void assertChainRefused(String chainPem, String expectedMessage) throws Exception {
        Map<String, Object> configs = brokerStoreWithChain(chainPem);
        KafkaException e = assertThrows(KafkaException.class, () -> factory(configs));
        assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
    }

    @Test
    public void clientReceivesFullChainEndingInTheRootWithItsThumbprint() throws Exception {
        OsCertStoreSslEngineFactory broker = factory(brokerStore());
        OsCertStoreSslEngineFactory teamB = factory(clientStore("teamb", "CN=teamb-app,O=TeamB", "CN=Team B CA,O=TeamB", teamBCaKeys, teamBCa, 1, 30));
        handshake(teamB, broker);
        Certificate[] received = lastClientEngine.getSession().getPeerCertificates();
        assertArrayEquals(new Certificate[]{server, intermediate, root}, received);
        // The client can recognise the root by its SHA-256 thumbprint.
        assertEquals(CertificateChainValidator.thumbprint(root), CertificateChainValidator.thumbprint((X509Certificate) received[2]));
        assertEquals(Arrays.asList("leaf", "intermediate", "root"), Arrays.asList(
            CertificateChainValidator.describe(Arrays.asList(server, intermediate, root)).stream()
                .map(l -> l.split(" ")[1]).toArray()));
    }

    @Test
    public void incompleteOrBrokenChainsAreRefused() throws Exception {
        assertChainRefused(pem(server), "a full chain is required");
        assertChainRefused(pem(server, intermediate), "is not a root CA");
        assertChainRefused(pem(server, root, intermediate), "order the chain");
        assertChainRefused(pem(intermediate, server, root), "order the chain");

        // An "intermediate" that is not a CA must not sign the leaf.
        KeyPair fakeKeys = TestSslUtils.generateKeyPair("RSA");
        X509Certificate fake = TestSslUtils.generateSignedCertificate("CN=Not A CA,O=Test", fakeKeys, 1, 30,
            "CN=Root CA,O=Test", rootKeys, ALG, false, true, true);
        KeyPair leafKeys = TestSslUtils.generateKeyPair("RSA");
        X509Certificate leaf = TestSslUtils.generateSignedCertificate("CN=localhost,O=Test", leafKeys, 1, 30,
            "CN=Not A CA,O=Test", fakeKeys, ALG, false, true, true);
        Map<String, Object> configs = brokerStoreWithChain(pem(leaf, fake, root));
        write(Paths.get(configs.get(OsCertStoreSslEngineFactory.KEY_DIR_CONFIG).toString()).resolve(DirectoryCertificateStore.KEY_FILE),
            pemKey(leafKeys));
        KafkaException e = assertThrows(KafkaException.class, () -> factory(configs));
        assertTrue(e.getMessage().contains("is not a CA"), e.getMessage());

        // A forged intermediate: same name as the real one, different key -> the leaf's signature doesn't verify.
        KeyPair otherKeys = TestSslUtils.generateKeyPair("RSA");
        X509Certificate forged = TestSslUtils.generateSignedCertificate("CN=Intermediate CA,O=Test", otherKeys, 1, 30,
            "CN=Root CA,O=Test", rootKeys, ALG, true, false, false);
        assertChainRefused(pem(server, forged, root), "does not verify");

        // The check can be turned off (e.g. peers that are configured with the intermediate themselves).
        Map<String, Object> leafOnly = brokerStoreWithChain(pem(server));
        leafOnly.put(OsCertStoreSslEngineFactory.REQUIRE_FULL_CHAIN_CONFIG, "false");
        assertEquals(1, factory(leafOnly).keystore().getCertificateChain("kafka").length);
    }

    @Test
    public void brokenChainOnReloadKeepsTheLastGoodOne() throws Exception {
        Map<String, Object> configs = brokerStore();
        configs.put(OsCertStoreSslEngineFactory.RELOAD_CHECK_INTERVAL_MS_CONFIG, "1000");
        MockTime time = new MockTime();
        OsCertStoreSslEngineFactory broker = new OsCertStoreSslEngineFactory(time);
        broker.configure(configs);
        write(Paths.get(configs.get(OsCertStoreSslEngineFactory.KEY_DIR_CONFIG).toString()).resolve(DirectoryCertificateStore.CHAIN_FILE),
            pem(server));
        time.sleep(1001);
        assertFalse(broker.maybeReload());
        assertEquals(3, broker.keystore().getCertificateChain("kafka").length);
    }

    @Test
    public void missingTrustDirIsAConfigError() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3");
        assertThrows(org.apache.kafka.common.config.ConfigException.class, () -> factory(configs));
        configs.put(OsCertStoreSslEngineFactory.TYPE_CONFIG, "KEYCHAIN");
        assertThrows(org.apache.kafka.common.config.ConfigException.class, () -> factory(configs));
        configs.put(OsCertStoreSslEngineFactory.TYPE_CONFIG, "WINDOWS");
        if (!System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).startsWith("windows")) {
            KafkaException e = assertThrows(KafkaException.class, () -> factory(configs));
            assertTrue(e.getMessage().contains("only available on Windows"), e.getMessage());
        }
    }

    @Test
    public void peerUnverifiedWithoutClientCertificate() throws Exception {
        // A client without its own certificate is rejected when client auth is required.
        OsCertStoreSslEngineFactory broker = factory(brokerStore());
        OsCertStoreSslEngineFactory anonymous = factory(store("anon", java.util.Collections.singletonMap("root.pem", pem(root)), null, null));
        assertThrows(SSLException.class, () -> {
            SSLEngine s = handshake(anonymous, broker);
            try {
                s.getSession().getPeerPrincipal();
            } catch (SSLPeerUnverifiedException e) {
                throw new SSLException(e);
            }
        });
    }
}
