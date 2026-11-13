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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiIdentityPrincipalTest {

    private static MultiIdentityPrincipal token(String... claims) {
        return new MultiIdentityPrincipal(KafkaPrincipal.USER_TYPE, "alice@example.com",
            MultiIdentityPrincipal.Source.TOKEN, Arrays.asList(claims));
    }

    @Test
    public void testAclPrincipalsAreThePrincipalPlusOnePerClaim() {
        MultiIdentityPrincipal p = token("kafka-readers", "urn:example:kafka:ops");
        assertEquals(new LinkedHashSet<>(Arrays.asList(
            new KafkaPrincipal("User", "alice@example.com"),
            new KafkaPrincipal("Claim", "kafka-readers"),
            new KafkaPrincipal("Claim", "urn:example:kafka:ops"))), p.aclPrincipals());
        // plain KafkaPrincipal instances, so they equal ACL entry principals
        for (KafkaPrincipal acl : p.aclPrincipals())
            assertEquals(KafkaPrincipal.class, acl.getClass());
    }

    @Test
    public void testAclPrincipalTypePerSource() {
        assertEquals("Claim", MultiIdentityPrincipal.Source.TOKEN.aclPrincipalType());
        assertEquals("Certificate", MultiIdentityPrincipal.Source.CERTIFICATE.aclPrincipalType());
        assertEquals("User", MultiIdentityPrincipal.Source.SASL_USER.aclPrincipalType());
    }

    @Test
    public void testIdentitiesDeduplicatedBlankRemovedAndUnmodifiable() {
        MultiIdentityPrincipal p = token("b", "a", "b", " ", "");
        assertEquals(Arrays.asList("b", "a"), p.identities());
        assertThrows(UnsupportedOperationException.class, () -> p.identities().add("c"));
    }

    @Test
    public void testToStringAndNameUnchanged() {
        MultiIdentityPrincipal p = token("secret-group");
        assertEquals("User:alice@example.com", p.toString());
        assertEquals("alice@example.com", p.getName());
        assertTrue(p.describe().contains("TOKEN"));
        assertTrue(p.describe().contains("1 identities"));
        assertFalse(p.describe().contains("secret-group"));
    }

    @Test
    public void testEqualsIncludesIdentities() {
        assertEquals(token("a", "b"), token("a", "b"));
        assertEquals(token("a", "b").hashCode(), token("a", "b").hashCode());
        assertNotEquals(token("a"), token("a", "b"));
        assertNotEquals(new KafkaPrincipal("User", "alice@example.com"), token());
    }

    @Test
    public void testNullArgumentsRejected() {
        assertThrows(NullPointerException.class, () -> new MultiIdentityPrincipal("User", "a", null, Collections.emptyList()));
        assertThrows(NullPointerException.class,
            () -> new MultiIdentityPrincipal("User", "a", MultiIdentityPrincipal.Source.TOKEN, null));
    }
}
