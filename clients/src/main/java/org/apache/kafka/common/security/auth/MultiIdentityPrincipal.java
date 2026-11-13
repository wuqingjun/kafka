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
package org.apache.kafka.common.security.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A principal that carries, besides its own {@code type:name}, a list of additional identities that come from
 * one authentication source, for example the claims of a SAML token.
 * <p>
 * Authorization ({@code AclAuthorizer}) treats the principal as the set {@link #aclPrincipals()}: the principal
 * itself plus one ACL principal per identity. A request is allowed if any of them has a matching ALLOW ACL and none
 * has a matching DENY ACL.
 * <p>
 * The name ({@link #getName()}) stays the "who" (e.g. the token subject); it is what quotas, logs and audit use.
 * {@link #toString()} is unchanged ({@code type:name}), so the identities never leak into places that use the
 * principal string.
 *
 * <table>
 *   <caption>ACL principal type per source</caption>
 *   <tr><th>Source</th><th>Identities</th><th>ACL principal</th></tr>
 *   <tr><td>{@link Source#TOKEN}</td><td>token claims</td><td>{@code Claim:<claim>}</td></tr>
 *   <tr><td>{@link Source#CERTIFICATE}</td><td>certificate CN and SANs</td><td>{@code Certificate:<name>}</td></tr>
 *   <tr><td>{@link Source#SASL_USER}</td><td>SASL login names</td><td>{@code User:<name>}</td></tr>
 * </table>
 */
public class MultiIdentityPrincipal extends KafkaPrincipal {

    /** ACL principal type for token claims, e.g. {@code Claim:kafka-readers}. */
    public static final String CLAIM_TYPE = "Claim";
    /** ACL principal type for certificate names, e.g. {@code Certificate:spiffe://example.org/sa/admin}. */
    public static final String CERTIFICATE_TYPE = "Certificate";

    /** Where the identities came from. */
    public enum Source {
        SASL_USER(KafkaPrincipal.USER_TYPE),
        CERTIFICATE(CERTIFICATE_TYPE),
        TOKEN(CLAIM_TYPE);

        private final String aclPrincipalType;

        Source(String aclPrincipalType) {
            this.aclPrincipalType = aclPrincipalType;
        }

        /** The principal type used in ACLs for identities from this source. */
        public String aclPrincipalType() {
            return aclPrincipalType;
        }
    }

    private final Source source;
    private final List<String> identities;

    /**
     * @param principalType type of the principal itself (usually {@link KafkaPrincipal#USER_TYPE})
     * @param name          the principal name, e.g. the token subject
     * @param source        where {@code identities} came from
     * @param identities    additional identities (order kept, duplicates and blank values removed)
     */
    public MultiIdentityPrincipal(String principalType, String name, Source source, List<String> identities) {
        this(principalType, name, source, identities, false);
    }

    public MultiIdentityPrincipal(String principalType, String name, Source source, List<String> identities,
                                  boolean tokenAuthenticated) {
        super(principalType, name, tokenAuthenticated);
        this.source = Objects.requireNonNull(source, "source");
        Set<String> unique = new LinkedHashSet<>();
        for (String identity : Objects.requireNonNull(identities, "identities")) {
            if (identity != null && !identity.trim().isEmpty())
                unique.add(identity);
        }
        this.identities = Collections.unmodifiableList(new ArrayList<>(unique));
    }

    public Source source() {
        return source;
    }

    /** The additional identities, e.g. the token claims. Unmodifiable. */
    public List<String> identities() {
        return identities;
    }

    /**
     * The principals to match against ACLs: this principal as a plain {@link KafkaPrincipal} ({@code type:name}),
     * plus {@code <source ACL type>:<identity>} for each identity. All elements are plain {@link KafkaPrincipal}
     * instances, so they compare equal to the principals in ACL entries.
     */
    public Set<KafkaPrincipal> aclPrincipals() {
        Set<KafkaPrincipal> principals = new LinkedHashSet<>();
        principals.add(new KafkaPrincipal(getPrincipalType(), getName()));
        for (String identity : identities)
            principals.add(new KafkaPrincipal(source.aclPrincipalType(), identity));
        return principals;
    }

    /** Human-readable form for logs: principal, source and number of identities (not their values). */
    public String describe() {
        return super.toString() + " (" + source + ", " + identities.size() + " identities)";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        MultiIdentityPrincipal that = (MultiIdentityPrincipal) o;
        return source == that.source && identities.equals(that.identities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), source, identities);
    }
}
