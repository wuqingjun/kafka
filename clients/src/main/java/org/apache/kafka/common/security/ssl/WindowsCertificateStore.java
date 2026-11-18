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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The Windows certificate store, through Java's built-in {@code SunMSCAPI} provider (no passwords; access controlled
 * and audited by Windows):
 * <ul>
 *   <li>trusted CAs from {@code Windows-ROOT} (the "Trusted Root Certification Authorities" store) or another store
 *       type, e.g. {@code Windows-ROOT-LOCALMACHINE} (Java 17+)</li>
 *   <li>own certificate chain and private key from {@code Windows-MY} ("Personal"), by alias (the certificate's
 *       friendly name)</li>
 * </ul>
 * Works only on Windows; elsewhere {@link #load()} fails with a clear message.
 */
public class WindowsCertificateStore implements OsCertificateStore {
    public static final String DEFAULT_TRUST_STORE = "Windows-ROOT";
    public static final String KEY_STORE = "Windows-MY";

    private final String trustStoreType;
    private final String keyAlias;

    /**
     * @param trustStoreType Windows store type with the trusted CAs, e.g. {@code Windows-ROOT}
     * @param keyAlias alias of the own certificate in {@code Windows-MY}, or null for a trust-only store
     */
    public WindowsCertificateStore(String trustStoreType, String keyAlias) {
        this.trustStoreType = trustStoreType;
        this.keyAlias = keyAlias;
    }

    @Override
    public Material load() throws IOException, GeneralSecurityException {
        if (!System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).startsWith("windows"))
            throw new GeneralSecurityException("The Windows certificate store is only available on Windows");
        KeyStore trust = KeyStore.getInstance(trustStoreType);
        trust.load(null, null);
        List<TrustedCa> cas = new ArrayList<>();
        for (String alias : Collections.list(trust.aliases())) {
            Certificate certificate = trust.getCertificate(alias);
            if (certificate instanceof X509Certificate)
                cas.add(new TrustedCa(trustStoreType + ":" + alias, (X509Certificate) certificate));
        }
        if (cas.isEmpty())
            throw new GeneralSecurityException("No CA certificate in " + trustStoreType);
        if (keyAlias == null)
            return new Material(cas, null, new ArrayList<>());
        KeyStore my = KeyStore.getInstance(KEY_STORE);
        my.load(null, null);
        Key key = my.getKey(keyAlias, null);
        if (!(key instanceof PrivateKey))
            throw new GeneralSecurityException("No private key for alias '" + keyAlias + "' in " + KEY_STORE);
        Certificate[] chain = my.getCertificateChain(keyAlias);
        List<X509Certificate> certificates = new ArrayList<>();
        if (chain != null) {
            for (Certificate certificate : chain)
                certificates.add((X509Certificate) certificate);
        }
        if (certificates.isEmpty())
            throw new GeneralSecurityException("No certificate chain for alias '" + keyAlias + "' in " + KEY_STORE);
        return new Material(cas, (PrivateKey) key, certificates);
    }

    @Override
    public String describe() {
        return "Windows stores trust=" + trustStoreType + (keyAlias != null ? ", key=" + KEY_STORE + ":" + keyAlias : "");
    }
}
