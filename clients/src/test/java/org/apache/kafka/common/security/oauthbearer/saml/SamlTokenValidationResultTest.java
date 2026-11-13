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
package org.apache.kafka.common.security.oauthbearer.saml;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SamlTokenValidationResultTest {

    @Test
    public void testKeepsOrderAndRemovesDuplicates() {
        SamlTokenValidationResult result = new SamlTokenValidationResult("alice",
            Arrays.asList("b", "a", "b", "c"), 123L);
        assertEquals("alice", result.subject());
        assertEquals(Arrays.asList("b", "a", "c"), result.claims());
        assertEquals(123L, result.expiresAtMs().getAsLong());
    }

    @Test
    public void testClaimsAreUnmodifiableAndCopied() {
        List<String> claims = new ArrayList<>(Collections.singletonList("a"));
        SamlTokenValidationResult result = new SamlTokenValidationResult("alice", claims, null);
        claims.add("b");
        assertEquals(Collections.singletonList("a"), result.claims());
        assertThrows(UnsupportedOperationException.class, () -> result.claims().add("c"));
    }

    @Test
    public void testNoExpiryAndEmptyClaimsAllowed() {
        SamlTokenValidationResult result = new SamlTokenValidationResult("alice", Collections.emptyList(), null);
        assertFalse(result.expiresAtMs().isPresent());
        assertTrue(result.claims().isEmpty());
    }

    @Test
    public void testRejectsBadInput() {
        assertThrows(IllegalArgumentException.class,
            () -> new SamlTokenValidationResult(null, Collections.emptyList(), null));
        assertThrows(IllegalArgumentException.class,
            () -> new SamlTokenValidationResult("  ", Collections.emptyList(), null));
        assertThrows(NullPointerException.class, () -> new SamlTokenValidationResult("alice", null, null));
        assertThrows(IllegalArgumentException.class,
            () -> new SamlTokenValidationResult("alice", Arrays.asList("a", null), null));
        assertThrows(IllegalArgumentException.class,
            () -> new SamlTokenValidationResult("alice", Arrays.asList("a", " "), null));
    }

    @Test
    public void testToStringHidesClaimValues() {
        String text = new SamlTokenValidationResult("alice", Arrays.asList("secret-group"), 5L).toString();
        assertTrue(text.contains("alice"));
        assertTrue(text.contains("claims=1"));
        assertFalse(text.contains("secret-group"));
    }
}
