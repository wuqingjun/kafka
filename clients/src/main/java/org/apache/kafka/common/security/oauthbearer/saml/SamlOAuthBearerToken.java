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

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A SAML token in Kafka's {@link OAuthBearerToken} form.
 * <ul>
 *   <li>{@link #value()}: the base64-encoded SAML token as sent by the client (a credential: never logged)</li>
 *   <li>{@link #principalName()}: the token subject; becomes the Kafka principal name</li>
 *   <li>{@link #scope()}: the token claims</li>
 *   <li>{@link #lifetimeMs()}: when the token expires</li>
 * </ul>
 * On the broker it is available after authentication as the SASL negotiated property {@code OAUTHBEARER.token}.
 */
public final class SamlOAuthBearerToken implements OAuthBearerToken {

    private final String value;
    private final String principalName;
    private final Set<String> claims;
    private final long lifetimeMs;
    private final Long startTimeMs;

    /**
     * @param value         the base64-encoded SAML token; must not be null or empty
     * @param principalName the token subject; must not be null or blank
     * @param claims        the token claims (order kept, duplicates removed); must not be null
     * @param lifetimeMs    expiry time, epoch milliseconds
     * @param startTimeMs   when the token became valid, epoch milliseconds, or null if unknown
     */
    public SamlOAuthBearerToken(String value, String principalName, Collection<String> claims, long lifetimeMs,
                                Long startTimeMs) {
        if (value == null || value.isEmpty())
            throw new IllegalArgumentException("token value must not be null or empty");
        if (principalName == null || principalName.trim().isEmpty())
            throw new IllegalArgumentException("principal name must not be null or blank");
        this.value = value;
        this.principalName = principalName;
        this.claims = Collections.unmodifiableSet(new LinkedHashSet<>(Objects.requireNonNull(claims, "claims")));
        this.lifetimeMs = lifetimeMs;
        this.startTimeMs = startTimeMs;
    }

    @Override
    public String value() {
        return value;
    }

    /** The token claims, in token order. Unmodifiable. */
    @Override
    public Set<String> scope() {
        return claims;
    }

    @Override
    public long lifetimeMs() {
        return lifetimeMs;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }

    @Override
    public String toString() {
        // Never include value(): it is a bearer credential. Claims can be many and sensitive: count only.
        return "SamlOAuthBearerToken(principalName=" + principalName + ", claims=" + claims.size()
            + ", lifetimeMs=" + lifetimeMs + ", startTimeMs=" + startTimeMs + ")";
    }
}
