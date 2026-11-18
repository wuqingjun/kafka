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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslClientAuth;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

/**
 * An {@link SslEngineFactory} ({@code ssl.engine.factory.class}) that takes the certificates and the private key from a
 * certificate store protected by the operating system instead of password-protected keystore / truststore files:
 * a permission-protected folder ({@link DirectoryCertificateStore}) or the Windows certificate store
 * ({@link WindowsCertificateStore}).
 * <ul>
 *   <li>Trust: {@link MultiCaTrustManager} — the peer is accepted if any one CA in the store validates its chain.</li>
 *   <li>Key: the own certificate chain (leaf, then intermediates) and private key; the whole chain is sent to the peer.</li>
 *   <li>Changes to the store are picked up without restart: on a new connection, at most every
 *       {@code ssl.os.cert.store.reload.check.interval.ms}, the store is re-read; if it changed, new connections use the
 *       new content through a new SSL context (so earlier TLS sessions can't be resumed) and every added / removed CA is logged with its SHA-256 fingerprint. A store that can't be loaded
 *       (e.g. a file with too open permissions) is not used; the last good content stays.</li>
 * </ul>
 * <p>The own certificate chain must be complete — leaf, intermediate CA(s), root CA, each signed by the next — so that
 * peers receive the whole chain and can validate it from the root (see {@link CertificateChainValidator}); disable
 * with {@code ssl.os.cert.store.require.full.chain=false}.
 * <p>Standard settings still used: {@code ssl.protocol}, {@code ssl.provider}, {@code ssl.enabled.protocols},
 * {@code ssl.cipher.suites}, {@code ssl.client.auth}, {@code ssl.secure.random.implementation},
 * {@code ssl.keymanager.algorithm}, {@code ssl.trustmanager.algorithm}; {@code ssl.keystore.*} and
 * {@code ssl.truststore.*} are ignored.
 */
public class OsCertStoreSslEngineFactory implements SslEngineFactory {
    public static final String PREFIX = "ssl.os.cert.store.";
    public static final String TYPE_CONFIG = PREFIX + "type";
    public static final String TRUST_DIR_CONFIG = PREFIX + "trust.dir";
    public static final String KEY_DIR_CONFIG = PREFIX + "key.dir";
    public static final String WINDOWS_TRUST_STORE_CONFIG = PREFIX + "windows.trust.store";
    public static final String WINDOWS_KEY_ALIAS_CONFIG = PREFIX + "windows.key.alias";
    public static final String PERMISSION_CHECK_CONFIG = PREFIX + "permission.check";
    public static final String RELOAD_CHECK_INTERVAL_MS_CONFIG = PREFIX + "reload.check.interval.ms";
    /** true (default): refuse an own certificate chain that isn't complete (leaf -> intermediate(s) -> root). */
    public static final String REQUIRE_FULL_CHAIN_CONFIG = PREFIX + "require.full.chain";
    public static final long DEFAULT_RELOAD_CHECK_INTERVAL_MS = 30_000L;
    private static final String KEY_ALIAS = "kafka";
    private static final Logger log = LoggerFactory.getLogger(OsCertStoreSslEngineFactory.class);
    private static final SecureRandom PASSWORD_RANDOM = new SecureRandom();

    private final Time time;
    private Map<String, Object> configs;
    private OsCertificateStore store;
    private long reloadIntervalMs;
    private boolean requireFullChain;
    private String protocol;
    private String provider;
    private String[] cipherSuites;
    private String[] enabledProtocols;
    private SecureRandom secureRandom;
    private SslClientAuth clientAuth;
    private String kmfAlgorithm;
    private String tmfAlgorithm;

    private volatile Loaded current;
    private volatile long nextCheckMs;

    /** The store's content as used by the TLS managers. */
    private static final class Loaded {
        final OsCertificateStore.Material material;
        final String fingerprint;
        final X509ExtendedKeyManager keyManager;
        final MultiCaTrustManager trustManager;
        final KeyStore keystore;
        final KeyStore truststore;
        final SSLContext sslContext;

        Loaded(OsCertificateStore.Material material, X509ExtendedKeyManager keyManager, MultiCaTrustManager trustManager,
               KeyStore keystore, KeyStore truststore, SSLContext sslContext) {
            this.material = material;
            this.fingerprint = material.fingerprint();
            this.keyManager = keyManager;
            this.trustManager = trustManager;
            this.keystore = keystore;
            this.truststore = truststore;
            this.sslContext = sslContext;
        }
    }

    public OsCertStoreSslEngineFactory() {
        this(Time.SYSTEM);
    }

    OsCertStoreSslEngineFactory(Time time) {
        this.time = time;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = Collections.unmodifiableMap(new HashMap<>(configs));
        this.store = createStore(configs);
        this.reloadIntervalMs = parseLong(configs.get(RELOAD_CHECK_INTERVAL_MS_CONFIG), DEFAULT_RELOAD_CHECK_INTERVAL_MS);
        this.requireFullChain = Boolean.parseBoolean(valueOr(configs.get(REQUIRE_FULL_CHAIN_CONFIG), "true"));
        this.protocol = (String) configs.get(SslConfigs.SSL_PROTOCOL_CONFIG);
        this.provider = (String) configs.get(SslConfigs.SSL_PROVIDER_CONFIG);
        List<String> ciphers = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        this.cipherSuites = ciphers != null && !ciphers.isEmpty() ? ciphers.toArray(new String[0]) : null;
        List<String> protocols = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        this.enabledProtocols = protocols != null && !protocols.isEmpty() ? protocols.toArray(new String[0]) : null;
        String random = (String) configs.get(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        SslClientAuth auth = SslClientAuth.forConfig((String) configs.get(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG));
        this.clientAuth = auth != null ? auth : SslClientAuth.NONE;
        this.kmfAlgorithm = valueOr(configs.get(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG), KeyManagerFactory.getDefaultAlgorithm());
        this.tmfAlgorithm = valueOr(configs.get(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG), TrustManagerFactory.getDefaultAlgorithm());
        try {
            this.secureRandom = random != null ? SecureRandom.getInstance(random) : null;
            Loaded loaded = load();
            install(loaded);
            log.info("Certificate store {} loaded: {} trusted CA(s){}", store.describe(), loaded.material.trustedCas().size(),
                loaded.material.privateKey() != null ? ", own certificate " + subject(loaded.material.certificateChain().get(0))
                    + " (chain of " + loaded.material.certificateChain().size() + ")" : ", no own certificate");
            for (OsCertificateStore.TrustedCa ca : loaded.material.trustedCas())
                log.info("Trusted CA: {} sha256={}", ca, ca.fingerprint());
            for (String line : CertificateChainValidator.describe(loaded.material.certificateChain()))
                log.info("Own certificate chain sent to peers: {}", line);
        } catch (IOException | GeneralSecurityException e) {
            throw new KafkaException("Failed to load the certificate store " + store.describe() + ": " + e.getMessage(), e);
        }
        this.nextCheckMs = time.milliseconds() + reloadIntervalMs;
    }

    static OsCertificateStore createStore(Map<String, ?> configs) {
        String type = valueOr(configs.get(TYPE_CONFIG), "DIRECTORY").toUpperCase(Locale.ROOT);
        switch (type) {
            case "DIRECTORY": {
                Object trustDir = configs.get(TRUST_DIR_CONFIG);
                if (trustDir == null || trustDir.toString().trim().isEmpty())
                    throw new ConfigException(TRUST_DIR_CONFIG + " is required for " + TYPE_CONFIG + "=DIRECTORY");
                Object keyDir = configs.get(KEY_DIR_CONFIG);
                boolean check = Boolean.parseBoolean(valueOr(configs.get(PERMISSION_CHECK_CONFIG), "true"));
                return new DirectoryCertificateStore(Paths.get(trustDir.toString().trim()),
                    keyDir == null || keyDir.toString().trim().isEmpty() ? null : Paths.get(keyDir.toString().trim()), check);
            }
            case "WINDOWS": {
                Object alias = configs.get(WINDOWS_KEY_ALIAS_CONFIG);
                return new WindowsCertificateStore(valueOr(configs.get(WINDOWS_TRUST_STORE_CONFIG), WindowsCertificateStore.DEFAULT_TRUST_STORE),
                    alias == null || alias.toString().trim().isEmpty() ? null : alias.toString().trim());
            }
            default:
                throw new ConfigException(TYPE_CONFIG, type, "must be DIRECTORY or WINDOWS");
        }
    }

    private static String valueOr(Object value, String fallback) {
        return value == null || value.toString().trim().isEmpty() ? fallback : value.toString().trim();
    }

    private static long parseLong(Object value, long fallback) {
        try {
            return value == null ? fallback : Long.parseLong(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(RELOAD_CHECK_INTERVAL_MS_CONFIG, value, "must be a number of milliseconds");
        }
    }

    private Loaded load() throws IOException, GeneralSecurityException {
        OsCertificateStore.Material material = store.load();
        if (material.privateKey() != null && requireFullChain)
            CertificateChainValidator.validateFullChain(material.certificateChain());
        MultiCaTrustManager trust = new MultiCaTrustManager(material.trustedCas(), tmfAlgorithm);
        KeyStore truststore = KeyStore.getInstance("PKCS12");
        truststore.load(null, null);
        int i = 0;
        for (OsCertificateStore.TrustedCa ca : material.trustedCas())
            truststore.setCertificateEntry("ca-" + i++, ca.certificate());
        KeyStore keystore = null;
        X509ExtendedKeyManager keys = null;
        if (material.privateKey() != null) {
            // In memory only: KeyManagerFactory needs a password for the key entry; a random one never leaves this method.
            char[] password = randomPassword();
            keystore = KeyStore.getInstance("PKCS12");
            keystore.load(null, null);
            List<X509Certificate> chain = material.certificateChain();
            keystore.setKeyEntry(KEY_ALIAS, material.privateKey(), password, chain.toArray(new X509Certificate[0]));
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
            kmf.init(keystore, password);
            for (KeyManager manager : kmf.getKeyManagers()) {
                if (manager instanceof X509ExtendedKeyManager)
                    keys = (X509ExtendedKeyManager) manager;
            }
            if (keys == null)
                throw new GeneralSecurityException("No X509ExtendedKeyManager from " + kmfAlgorithm);
        }
        // A new SSL context per store content: its session cache and ticket keys are new too, so sessions established
        // under the previous content (e.g. with a CA that was removed since) can't be resumed without full validation.
        SSLContext context = provider != null ? SSLContext.getInstance(protocol, provider) : SSLContext.getInstance(protocol);
        context.init(keys != null ? new KeyManager[]{keys} : null, new TrustManager[]{trust}, secureRandom);
        return new Loaded(material, keys, trust, keystore, truststore, context);
    }

    private static char[] randomPassword() {
        char[] password = new char[32];
        for (int i = 0; i < password.length; i++)
            password[i] = (char) ('a' + PASSWORD_RANDOM.nextInt(26));
        return password;
    }

    private void install(Loaded loaded) {
        current = loaded;
    }

    /**
     * Re-read the store if the check interval passed; switch to the new content if it changed and is valid.
     *
     * @return true if new content was installed
     */
    boolean maybeReload() {
        long now = time.milliseconds();
        if (now < nextCheckMs)
            return false;
        synchronized (this) {
            if (now < nextCheckMs)
                return false;
            nextCheckMs = now + reloadIntervalMs;
            Loaded previous = current;
            Loaded next;
            try {
                OsCertificateStore.Material material = store.load();
                if (material.fingerprint().equals(previous.fingerprint))
                    return false;
                next = load();
            } catch (IOException | GeneralSecurityException | RuntimeException e) {
                log.error("Certificate store {} could not be reloaded; keeping the last good content: {}", store.describe(), e.getMessage());
                return false;
            }
            if (previous.material.privateKey() != null && next.material.privateKey() == null) {
                log.error("Certificate store {} no longer has an own certificate; keeping the last good content", store.describe());
                return false;
            }
            logChanges(previous.material, next.material);
            install(next);
            return true;
        }
    }

    private void logChanges(OsCertificateStore.Material before, OsCertificateStore.Material after) {
        Map<String, OsCertificateStore.TrustedCa> old = new HashMap<>();
        before.trustedCas().forEach(ca -> old.put(ca.fingerprint(), ca));
        Map<String, OsCertificateStore.TrustedCa> now = new HashMap<>();
        after.trustedCas().forEach(ca -> now.put(ca.fingerprint(), ca));
        now.forEach((fp, ca) -> {
            if (!old.containsKey(fp))
                log.info("Certificate store {}: trusted CA ADDED {} sha256={}", store.describe(), ca, fp);
        });
        old.forEach((fp, ca) -> {
            if (!now.containsKey(fp))
                log.info("Certificate store {}: trusted CA REMOVED {} sha256={}", store.describe(), ca, fp);
        });
        if (!before.certificateChain().equals(after.certificateChain()) || !Objects.equals(before.privateKey(), after.privateKey()))
            CertificateChainValidator.describe(after.certificateChain()).forEach(line ->
                log.info("Certificate store {}: own certificate chain changed: {}", store.describe(), line));
        log.info("Certificate store {} reloaded: {} trusted CA(s)", store.describe(), after.trustedCas().size());
    }

    private static String subject(X509Certificate certificate) {
        return certificate.getSubjectX500Principal().getName();
    }

    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        maybeReload();
        SSLEngine engine = createEngine(peerHost, peerPort);
        engine.setUseClientMode(true);
        SSLParameters parameters = engine.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm(endpointIdentification);
        engine.setSSLParameters(parameters);
        return engine;
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        maybeReload();
        SSLEngine engine = createEngine(peerHost, peerPort);
        engine.setUseClientMode(false);
        if (clientAuth == SslClientAuth.REQUIRED)
            engine.setNeedClientAuth(true);
        else if (clientAuth == SslClientAuth.REQUESTED)
            engine.setWantClientAuth(true);
        return engine;
    }

    private SSLEngine createEngine(String peerHost, int peerPort) {
        SSLEngine engine = current.sslContext.createSSLEngine(peerHost, peerPort);
        if (cipherSuites != null)
            engine.setEnabledCipherSuites(cipherSuites);
        if (enabledProtocols != null)
            engine.setEnabledProtocols(enabledProtocols);
        return engine;
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        return !nextConfigs.equals(configs);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return SslConfigs.RECONFIGURABLE_CONFIGS;
    }

    @Override
    public KeyStore keystore() {
        Loaded loaded = current;
        return loaded != null ? loaded.keystore : null;
    }

    @Override
    public KeyStore truststore() {
        Loaded loaded = current;
        return loaded != null ? loaded.truststore : null;
    }

    /**
     * @return the CAs currently trusted, in the order they are tried
     */
    public List<OsCertificateStore.TrustedCa> trustedCas() {
        return current.trustManager.cas();
    }

    @Override
    public void close() {
        // Nothing to release: the store is re-read on demand and holds no open resources.
    }
}
