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

import java.security.GeneralSecurityException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks that an own certificate chain is complete, so that the peer receives <i>leaf &rarr; intermediate(s) &rarr;
 * root</i> and can validate it starting from the root (e.g. by comparing the root's SHA-256 thumbprint):
 * <ol>
 *   <li>at least two certificates: the leaf first, the root last</li>
 *   <li>each certificate is issued by the next one (issuer = next subject) and its signature verifies with the next
 *       one's public key</li>
 *   <li>every certificate between the leaf and the root is a CA ({@code basicConstraints CA:true}) and its path
 *       length constraint allows the certificates below it</li>
 *   <li>the last certificate is self-signed (a root)</li>
 *   <li>every certificate is currently valid (not expired, not before its start date)</li>
 * </ol>
 */
public final class CertificateChainValidator {

    private CertificateChainValidator() {
    }

    /**
     * @param chain the own certificate first, then its issuers up to the root
     * @throws GeneralSecurityException describing the first problem found
     */
    public static void validateFullChain(List<X509Certificate> chain) throws GeneralSecurityException {
        if (chain.size() < 2)
            throw new GeneralSecurityException("The certificate chain has " + chain.size() + " certificate(s); a full " +
                "chain is required: own certificate first, then the intermediate CA(s), then the root CA (" +
                "e.g. cat leaf.crt intermediate-ca.crt root-ca.crt > chain.pem)");
        for (int i = 0; i < chain.size(); i++) {
            X509Certificate cert = chain.get(i);
            try {
                cert.checkValidity();
            } catch (CertificateException e) {
                throw new GeneralSecurityException("Certificate [" + i + "] " + subject(cert) + " is not valid now: " + e.getMessage(), e);
            }
            if (i + 1 < chain.size()) {
                X509Certificate issuer = chain.get(i + 1);
                if (!cert.getIssuerX500Principal().equals(issuer.getSubjectX500Principal()))
                    throw new GeneralSecurityException("Certificate [" + i + "] " + subject(cert) + " is issued by " +
                        cert.getIssuerX500Principal().getName() + ", but the next certificate [" + (i + 1) + "] is " +
                        subject(issuer) + "; order the chain leaf -> intermediate(s) -> root");
                verify(cert, issuer, i);
                // Every issuer except the root must be a CA; its pathLen must allow the CAs below it (i of them minus the leaf).
                if (i + 1 < chain.size() - 1) {
                    int pathLen = issuer.getBasicConstraints();
                    if (pathLen < 0)
                        throw new GeneralSecurityException("Certificate [" + (i + 1) + "] " + subject(issuer) +
                            " signs another certificate but is not a CA (no basicConstraints CA:true)");
                    if (pathLen < i)
                        throw new GeneralSecurityException("Certificate [" + (i + 1) + "] " + subject(issuer) +
                            " has pathLen " + pathLen + ", too small for the " + i + " CA certificate(s) below it");
                }
            }
        }
        X509Certificate root = chain.get(chain.size() - 1);
        if (!root.getIssuerX500Principal().equals(root.getSubjectX500Principal()))
            throw new GeneralSecurityException("The last certificate [" + (chain.size() - 1) + "] " + subject(root) +
                " is not a root CA (issued by " + root.getIssuerX500Principal().getName() + "); append the root CA certificate");
        verify(root, root, chain.size() - 1);
    }

    private static void verify(X509Certificate cert, X509Certificate issuer, int index) throws GeneralSecurityException {
        try {
            cert.verify(issuer.getPublicKey());
        } catch (GeneralSecurityException e) {
            throw new GeneralSecurityException("The signature of certificate [" + index + "] " + subject(cert) +
                " does not verify with the key of " + subject(issuer) + ": " + e.getMessage(), e);
        }
    }

    /**
     * @param chain certificates
     * @return one line per certificate: index, subject and SHA-256 thumbprint (e.g. to publish the root's thumbprint)
     */
    public static List<String> describe(List<X509Certificate> chain) {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < chain.size(); i++) {
            X509Certificate cert = chain.get(i);
            String role = i == 0 ? "leaf" : i == chain.size() - 1 &&
                cert.getIssuerX500Principal().equals(cert.getSubjectX500Principal()) ? "root" : "intermediate";
            lines.add("[" + i + "] " + role + " " + subject(cert) + " sha256=" + thumbprint(cert));
        }
        return lines;
    }

    static String thumbprint(X509Certificate cert) {
        try {
            return OsCertificateStore.sha256Hex(cert.getEncoded());
        } catch (CertificateEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String subject(X509Certificate cert) {
        return cert.getSubjectX500Principal().getName();
    }
}
