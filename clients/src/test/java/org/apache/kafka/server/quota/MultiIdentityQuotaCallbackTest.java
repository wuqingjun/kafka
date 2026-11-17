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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.MultiIdentityPrincipal;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MultiIdentityQuotaCallbackTest {
    private static final String CC_DN = "O=LocalDev,CN=cruise-control";

    private final MultiIdentityQuotaCallback callback = new MultiIdentityQuotaCallback();

    /** A quota entity as Kafka passes it: one config entity. */
    private static ClientQuotaEntity entity(ClientQuotaEntity.ConfigEntityType type, String name) {
        ClientQuotaEntity.ConfigEntity configEntity = new ClientQuotaEntity.ConfigEntity() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ClientQuotaEntity.ConfigEntityType entityType() {
                return type;
            }

            @Override
            public String toString() {
                return type + ":" + name;
            }
        };
        return () -> Collections.singletonList(configEntity);
    }

    private static ClientQuotaEntity user(String identity) {
        return entity(ClientQuotaEntity.ConfigEntityType.USER, identity);
    }

    private static ClientQuotaEntity defaultUser() {
        return entity(ClientQuotaEntity.ConfigEntityType.DEFAULT_USER, "<default>");
    }

    private static MultiIdentityPrincipal token(String subject, String... claims) {
        return new MultiIdentityPrincipal("User", subject, MultiIdentityPrincipal.Source.TOKEN, Arrays.asList(claims));
    }

    private static Map<String, String> bucket(String identity) {
        return Collections.singletonMap(MultiIdentityQuotaCallback.IDENTITY_TAG, identity);
    }

    private static Map<String, String> defaultBucket(String principal) {
        Map<String, String> tags = new HashMap<>();
        tags.put(MultiIdentityQuotaCallback.IDENTITY_TAG, MultiIdentityQuotaCallback.DEFAULT_IDENTITY);
        tags.put(MultiIdentityQuotaCallback.PRINCIPAL_TAG, principal);
        return tags;
    }

    private Double limitFor(ClientQuotaType type, KafkaPrincipal principal) {
        return callback.quotaLimit(type, callback.quotaMetricTags(type, principal, "any-client"));
    }

    @Test
    public void testNoQuotasMeansDefaultBucketWithoutLimit() {
        KafkaPrincipal alice = token("alice@example.com", "payments-team");
        assertEquals(defaultBucket("User:alice@example.com"), callback.quotaMetricTags(ClientQuotaType.PRODUCE, alice, "c1"));
        assertNull(limitFor(ClientQuotaType.PRODUCE, alice));
    }

    @Test
    public void testClaimBucketIsSharedByTheTeam() {
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"), 5000);
        Map<String, String> alice = callback.quotaMetricTags(ClientQuotaType.PRODUCE, token("alice@example.com", "payments-team"), "a");
        Map<String, String> bob = callback.quotaMetricTags(ClientQuotaType.PRODUCE, token("bob@example.com", "payments-team", "x"), "b");
        assertEquals(bucket("Claim:payments-team"), alice);
        assertEquals(alice, bob);
        assertEquals(5000.0, callback.quotaLimit(ClientQuotaType.PRODUCE, alice));
    }

    @Test
    public void testHighestValueWinsAmongSeveralClaims() {
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"), 5000);
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:analytics"), 9000);
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:interns"), 100);
        KafkaPrincipal both = token("carol@example.com", "interns", "payments-team", "analytics");
        assertEquals(bucket("Claim:analytics"), callback.quotaMetricTags(ClientQuotaType.PRODUCE, both, "c"));
        assertEquals(9000.0, limitFor(ClientQuotaType.PRODUCE, both));

        // tie: the alphabetically first identity, independent of the claim order in the token
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"), 9000);
        assertEquals(bucket("Claim:analytics"), callback.quotaMetricTags(ClientQuotaType.PRODUCE, token("d", "payments-team", "analytics"), "c"));
    }

    @Test
    public void testQuotaTypesResolveIndependently() {
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"), 5000);
        callback.updateQuota(ClientQuotaType.FETCH, user("Claim:analytics"), 20000);
        callback.updateQuota(ClientQuotaType.FETCH, defaultUser(), 1000);
        KafkaPrincipal erin = token("erin@example.com", "payments-team", "analytics");
        assertEquals(bucket("Claim:payments-team"), callback.quotaMetricTags(ClientQuotaType.PRODUCE, erin, "c"));
        assertEquals(bucket("Claim:analytics"), callback.quotaMetricTags(ClientQuotaType.FETCH, erin, "c"));
        // no ClientQuotaType.REQUEST quota for any of her identities and no ClientQuotaType.REQUEST default: no limit
        assertNull(limitFor(ClientQuotaType.REQUEST, erin));
    }

    @Test
    public void testCertificatePrincipalAndSubjectEntries() {
        callback.updateQuota(ClientQuotaType.PRODUCE, user("User:" + CC_DN), 100000);
        callback.updateQuota(ClientQuotaType.PRODUCE, user("User:svc@example.com"), 7000);
        KafkaPrincipal cruiseControl = new KafkaPrincipal("User", CC_DN);
        assertEquals(bucket("User:" + CC_DN), callback.quotaMetricTags(ClientQuotaType.PRODUCE, cruiseControl, "cc"));
        assertEquals(100000.0, limitFor(ClientQuotaType.PRODUCE, cruiseControl));
        // a token subject can have its own entry too
        assertEquals(7000.0, limitFor(ClientQuotaType.PRODUCE, token("svc@example.com", "nothing-configured")));
    }

    @Test
    public void testDefaultIsPerPrincipal() {
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"), 5000);
        callback.updateQuota(ClientQuotaType.PRODUCE, defaultUser(), 100);
        KafkaPrincipal frank = token("frank@example.com", "unknown-team");
        KafkaPrincipal grace = new KafkaPrincipal("User", "O=Other,CN=grace");
        assertEquals(defaultBucket("User:frank@example.com"), callback.quotaMetricTags(ClientQuotaType.PRODUCE, frank, "f"));
        assertEquals(defaultBucket("User:O=Other,CN=grace"), callback.quotaMetricTags(ClientQuotaType.PRODUCE, grace, "g"));
        assertEquals(100.0, limitFor(ClientQuotaType.PRODUCE, frank));
        assertEquals(100.0, limitFor(ClientQuotaType.PRODUCE, grace));
    }

    @Test
    public void testClientIdDoesNotChangeTheBucket() {
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"), 5000);
        KafkaPrincipal alice = token("alice@example.com", "payments-team");
        Map<String, String> first = callback.quotaMetricTags(ClientQuotaType.PRODUCE, alice, "client-1");
        for (String clientId : Arrays.asList("client-2", "random-" + System.nanoTime(), ""))
            assertEquals(first, callback.quotaMetricTags(ClientQuotaType.PRODUCE, alice, clientId));
    }

    @Test
    public void testRemoveQuota() {
        callback.updateQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"), 5000);
        callback.updateQuota(ClientQuotaType.PRODUCE, defaultUser(), 100);
        KafkaPrincipal alice = token("alice@example.com", "payments-team");
        callback.removeQuota(ClientQuotaType.PRODUCE, user("Claim:payments-team"));
        assertEquals(defaultBucket("User:alice@example.com"), callback.quotaMetricTags(ClientQuotaType.PRODUCE, alice, "a"));
        assertEquals(100.0, limitFor(ClientQuotaType.PRODUCE, alice));
        callback.removeQuota(ClientQuotaType.PRODUCE, defaultUser());
        assertNull(limitFor(ClientQuotaType.PRODUCE, alice));
    }

    @Test
    public void testClientIdQuotasAreIgnored() {
        callback.updateQuota(ClientQuotaType.PRODUCE, entity(ClientQuotaEntity.ConfigEntityType.CLIENT_ID, "payments-app"), 10);
        callback.updateQuota(ClientQuotaType.PRODUCE, entity(ClientQuotaEntity.ConfigEntityType.DEFAULT_CLIENT_ID, "<default>"), 10);
        ClientQuotaEntity.ConfigEntity userPart = user("Claim:payments-team").configEntities().get(0);
        ClientQuotaEntity.ConfigEntity clientPart = entity(ClientQuotaEntity.ConfigEntityType.CLIENT_ID, "x").configEntities().get(0);
        callback.updateQuota(ClientQuotaType.PRODUCE, () -> Arrays.asList(userPart, clientPart), 10);
        assertNull(limitFor(ClientQuotaType.PRODUCE, token("alice@example.com", "payments-team")));
    }
}
