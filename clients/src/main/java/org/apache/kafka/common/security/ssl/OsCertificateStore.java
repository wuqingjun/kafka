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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A certificate store protected by the operating system rather than by a password: the CA certificates to trust and,
 * optionally, the own certificate chain and private key. Used by {@link OsCertStoreSslEngineFactory}.
 */
public interface OsCertificateStore {

    /**
     * Read the store.
     *
     * @return the CAs to trust and the own key material
     * @throws IOException if the store can't be read
     * @throws GeneralSecurityException if its content is invalid or the store fails its protection checks
     */
    Material load() throws IOException, GeneralSecurityException;

    /**
     * @return a short description for logs, e.g. the folder paths
     */
    String describe();

    /** A trusted CA certificate and where it came from. */
    final class TrustedCa {
        private final String source;
        private final X509Certificate certificate;

        public TrustedCa(String source, X509Certificate certificate) {
            this.source = source;
            this.certificate = certificate;
        }

        /** @return e.g. the file name or the store alias */
        public String source() {
            return source;
        }

        public X509Certificate certificate() {
            return certificate;
        }

        /** @return SHA-256 fingerprint of the certificate, hex */
        public String fingerprint() {
            try {
                return sha256Hex(certificate.getEncoded());
            } catch (CertificateEncodingException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String toString() {
            return certificate.getSubjectX500Principal().getName() + " (" + source + ", sha256 " + fingerprint().substring(0, 16) + "...)";
        }
    }

    /** What a store holds. */
    final class Material {
        private final List<TrustedCa> trustedCas;
        private final PrivateKey privateKey;
        private final List<X509Certificate> certificateChain;

        /**
         * @param trustedCas the CAs to trust
         * @param privateKey the own private key, or null for a trust-only store
         * @param certificateChain the own certificate first, then its issuers; empty for a trust-only store
         */
        public Material(List<TrustedCa> trustedCas, PrivateKey privateKey, List<X509Certificate> certificateChain) {
            this.trustedCas = Collections.unmodifiableList(new ArrayList<>(trustedCas));
            this.privateKey = privateKey;
            this.certificateChain = Collections.unmodifiableList(new ArrayList<>(certificateChain));
        }

        public List<TrustedCa> trustedCas() {
            return trustedCas;
        }

        public PrivateKey privateKey() {
            return privateKey;
        }

        public List<X509Certificate> certificateChain() {
            return certificateChain;
        }

        /** @return a fingerprint of everything in the store, to detect changes */
        public String fingerprint() {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                for (TrustedCa ca : trustedCas)
                    digest.update(ca.certificate().getEncoded());
                for (X509Certificate certificate : certificateChain)
                    digest.update(certificate.getEncoded());
                if (privateKey != null)
                    digest.update(privateKey.getEncoded());
                return hex(digest.digest());
            } catch (NoSuchAlgorithmException | CertificateEncodingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * @param bytes data
     * @return SHA-256 of the data, lowercase hex
     */
    static String sha256Hex(byte[] bytes) {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param bytes data
     * @return lowercase hex
     */
    static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes)
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        return sb.toString();
    }
}
