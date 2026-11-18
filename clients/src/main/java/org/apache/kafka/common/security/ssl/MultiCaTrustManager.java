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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Trusts a peer if <b>any one</b> of the configured CAs validates its certificate chain: the CAs are tried one by one,
 * each with Java's standard PKIX validation anchored on that CA alone (chain building through the intermediates the
 * peer sends, validity dates, key usage, signatures, and — in client mode — hostname verification). The first CA that
 * validates the chain accepts the peer; if none does, the handshake fails with the reasons of every attempt.
 */
public class MultiCaTrustManager extends X509ExtendedTrustManager {
    private static final Logger log = LoggerFactory.getLogger(MultiCaTrustManager.class);

    /** One CA and the validator anchored on it. */
    private static final class Anchor {
        private final OsCertificateStore.TrustedCa ca;
        private final X509ExtendedTrustManager validator;

        Anchor(OsCertificateStore.TrustedCa ca, X509ExtendedTrustManager validator) {
            this.ca = ca;
            this.validator = validator;
        }
    }

    @FunctionalInterface
    private interface Check {
        void run(X509ExtendedTrustManager validator) throws CertificateException;
    }

    private final List<Anchor> anchors;
    private final X509Certificate[] acceptedIssuers;

    /**
     * @param cas the CAs to trust, tried in this order
     * @param algorithm trust manager algorithm, e.g. {@code PKIX}
     * @throws GeneralSecurityException if a validator can't be created
     */
    public MultiCaTrustManager(List<OsCertificateStore.TrustedCa> cas, String algorithm) throws GeneralSecurityException {
        if (cas.isEmpty())
            throw new GeneralSecurityException("At least one CA is required");
        List<Anchor> list = new ArrayList<>();
        List<X509Certificate> issuers = new ArrayList<>();
        for (OsCertificateStore.TrustedCa ca : cas) {
            list.add(new Anchor(ca, validatorFor(ca.certificate(), algorithm)));
            issuers.add(ca.certificate());
        }
        this.anchors = Collections.unmodifiableList(list);
        this.acceptedIssuers = issuers.toArray(new X509Certificate[0]);
    }

    private static X509ExtendedTrustManager validatorFor(X509Certificate ca, String algorithm) throws GeneralSecurityException {
        KeyStore oneCa = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            oneCa.load(null, null);
        } catch (IOException e) {
            throw new GeneralSecurityException(e);
        }
        oneCa.setCertificateEntry("ca", ca);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm);
        factory.init(oneCa);
        for (TrustManager manager : factory.getTrustManagers()) {
            if (manager instanceof X509ExtendedTrustManager)
                return (X509ExtendedTrustManager) manager;
        }
        throw new GeneralSecurityException("No X509ExtendedTrustManager from " + algorithm);
    }

    /**
     * @return the CAs, in the order they are tried
     */
    public List<OsCertificateStore.TrustedCa> cas() {
        List<OsCertificateStore.TrustedCa> cas = new ArrayList<>();
        anchors.forEach(a -> cas.add(a.ca));
        return cas;
    }

    private void tryEach(X509Certificate[] chain, String role, Check check) throws CertificateException {
        List<String> failures = new ArrayList<>();
        for (Anchor anchor : anchors) {
            try {
                check.run(anchor.validator);
                if (log.isDebugEnabled())
                    log.debug("{} certificate {} validated by CA {}", role, subject(chain), anchor.ca);
                return;
            } catch (CertificateException e) {
                failures.add(anchor.ca.certificate().getSubjectX500Principal().getName() + ": " + e.getMessage());
            }
        }
        String message = String.format("%s certificate %s is not trusted by any of the %d CAs in the store", role,
            subject(chain), anchors.size());
        log.info("{} ({})", message, String.join("; ", failures));
        throw new CertificateException(message);
    }

    private static String subject(X509Certificate[] chain) {
        return chain == null || chain.length == 0 ? "(none)" : chain[0].getSubjectX500Principal().getName();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        tryEach(chain, "Client", v -> v.checkClientTrusted(chain, authType, engine));
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        tryEach(chain, "Server", v -> v.checkServerTrusted(chain, authType, engine));
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        tryEach(chain, "Client", v -> v.checkClientTrusted(chain, authType, socket));
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        tryEach(chain, "Server", v -> v.checkServerTrusted(chain, authType, socket));
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        tryEach(chain, "Client", v -> v.checkClientTrusted(chain, authType));
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        tryEach(chain, "Server", v -> v.checkServerTrusted(chain, authType));
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return acceptedIssuers.clone();
    }
}
