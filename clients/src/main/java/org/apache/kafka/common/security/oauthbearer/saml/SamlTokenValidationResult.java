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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Result of a successful {@link SamlTokenValidator#validate(String)}. Only valid tokens produce a result;
 * an invalid token throws {@link SamlTokenValidationException} instead.
 * <p>
 * Use in Kafka:
 * <ul>
 *   <li>{@link #subject()} becomes the principal name (used for quotas, logs and audit).</li>
 *   <li>{@link #claims()} are the identities checked against ACLs ({@code Claim:<value>}); access is granted if
 *       any claim is allowed and none is denied.</li>
 *   <li>{@link #expiresAtMs()}, if present, is when the token expires: an expired token is rejected, and Kafka ends
 *       the session at that time unless the client re-authenticates (on the same connection, with a fresh token)
 *       before — whether or not {@code connections.max.reauth.ms} is set.</li>
 * </ul>
 */
public final class SamlTokenValidationResult {

    private final String subject;
    private final List<String> claims;
    private final OptionalLong expiresAtMs;

    /**
     * @param subject     who the token was issued to (e.g. the SAML NameID); must not be null or blank
     * @param claims      the token's claims; may be empty, must not contain null or blank values. Order is kept,
     *                    duplicates are removed.
     * @param expiresAtMs when the token expires (epoch milliseconds), or {@code null} if the token has no expiry
     */
    public SamlTokenValidationResult(String subject, List<String> claims, Long expiresAtMs) {
        if (subject == null || subject.trim().isEmpty())
            throw new IllegalArgumentException("subject must not be null or blank");
        Objects.requireNonNull(claims, "claims must not be null");
        for (String claim : claims) {
            if (claim == null || claim.trim().isEmpty())
                throw new IllegalArgumentException("claims must not contain null or blank values");
        }
        this.subject = subject;
        this.claims = Collections.unmodifiableList(new ArrayList<>(new LinkedHashSet<>(claims)));
        this.expiresAtMs = expiresAtMs == null ? OptionalLong.empty() : OptionalLong.of(expiresAtMs);
    }

    /** Who the token was issued to. Never null or blank. */
    public String subject() {
        return subject;
    }

    /** The token's claims, in token order, without duplicates. Never null; may be empty. Unmodifiable. */
    public List<String> claims() {
        return claims;
    }

    /** When the token expires (epoch milliseconds), if it has an expiry. */
    public OptionalLong expiresAtMs() {
        return expiresAtMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SamlTokenValidationResult that = (SamlTokenValidationResult) o;
        return subject.equals(that.subject) && claims.equals(that.claims) && expiresAtMs.equals(that.expiresAtMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, claims, expiresAtMs);
    }

    @Override
    public String toString() {
        // Claims can be sensitive and numerous: log the count, not the values.
        return "SamlTokenValidationResult(subject=" + subject + ", claims=" + claims.size()
            + ", expiresAtMs=" + (expiresAtMs.isPresent() ? expiresAtMs.getAsLong() : "none") + ")";
    }
}
