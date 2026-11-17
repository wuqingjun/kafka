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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.MultiIdentityPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client quotas by security identity instead of {@code client.id}.
 * <p>
 * Quota values are stored as Kafka quotas of entity type {@code user}, where the user name is an identity string,
 * e.g. {@code Claim:payments-team} or {@code User:O=LocalDev,CN=cruise-control}, plus the default user entity for
 * principals without a matching identity. (Cruise Control's quota sync writes them from a signed file.)
 * <p>
 * For every request and quota type (produce, fetch, request time):
 * <ul>
 *   <li>The principal's identities are its own {@code Type:name} and, for a {@link MultiIdentityPrincipal} (e.g. a SAML
 *       token), each identity it carries (subject and claims, as used by ACLs).</li>
 *   <li>Among those with a value for this quota type, the <b>highest</b> value wins (claims grant capacity; ties go to
 *       the alphabetically first identity). Its bucket is <b>shared</b> by every principal that resolves to it: one
 *       budget per team claim and broker.</li>
 *   <li>No match: the default value, in a bucket <b>per principal</b>, so that one unknown client can't use up the
 *       budget of all others. No default either: no limit.</li>
 * </ul>
 * Quotas keyed on {@code client-id} are ignored (logged once). Kafka's own algorithm (sliding-window rate, throttle by
 * delaying the response) applies to the buckets.
 * <p>
 * Broker configuration: {@code client.quota.callback.class=org.apache.kafka.server.quota.MultiIdentityQuotaCallback}
 */
public class MultiIdentityQuotaCallback implements ClientQuotaCallback {
    /** Metric tag with the identity that owns the bucket, or {@link #DEFAULT_IDENTITY}. */
    public static final String IDENTITY_TAG = "identity";
    /** Metric tag with the principal, only for default buckets (one bucket per principal). */
    public static final String PRINCIPAL_TAG = "principal";
    /** Identity tag value of default buckets. */
    public static final String DEFAULT_IDENTITY = "<default>";
    private static final Logger log = LoggerFactory.getLogger(MultiIdentityQuotaCallback.class);

    private final Map<ClientQuotaType, Map<String, Double>> quotas = new EnumMap<>(ClientQuotaType.class);
    private final Map<ClientQuotaType, Double> defaults = new ConcurrentHashMap<>();
    private volatile boolean clientIdQuotaWarned;

    public MultiIdentityQuotaCallback() {
        for (ClientQuotaType type : ClientQuotaType.values())
            quotas.put(type, new ConcurrentHashMap<>());
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        String identity = resolve(quotaType, principal);
        Map<String, String> tags = new HashMap<>();
        if (identity != null) {
            tags.put(IDENTITY_TAG, identity);
        } else {
            tags.put(IDENTITY_TAG, DEFAULT_IDENTITY);
            tags.put(PRINCIPAL_TAG, identityOf(principal));
        }
        return tags;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        String identity = metricTags.get(IDENTITY_TAG);
        if (identity == null)
            return null;
        if (DEFAULT_IDENTITY.equals(identity))
            return defaults.get(quotaType);
        return quotas.get(quotaType).get(identity);
    }

    @Override
    public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        ClientQuotaEntity.ConfigEntity user = userEntity(quotaEntity);
        if (user == null)
            return;
        if (user.entityType() == ClientQuotaEntity.ConfigEntityType.DEFAULT_USER)
            defaults.put(quotaType, newValue);
        else
            quotas.get(quotaType).put(user.name(), newValue);
        log.info("{} quota for {} set to {}", quotaType, describe(user), newValue);
    }

    @Override
    public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        ClientQuotaEntity.ConfigEntity user = userEntity(quotaEntity);
        if (user == null)
            return;
        Double removed = user.entityType() == ClientQuotaEntity.ConfigEntityType.DEFAULT_USER
            ? defaults.remove(quotaType)
            : quotas.get(quotaType).remove(user.name());
        // Kafka also calls this for every quota type an entity doesn't have; only log real removals
        if (removed != null)
            log.info("{} quota for {} removed (was {})", quotaType, describe(user), removed);
        else
            log.debug("{} quota for {}: not set", quotaType, describe(user));
    }

    /**
     * Kafka re-checks all buckets after every quota update when a custom callback is used, and asks for the tags of
     * every request, so nothing extra is needed here.
     */
    @Override
    public boolean quotaResetRequired(ClientQuotaType quotaType) {
        return false;
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {
    }

    /**
     * @return The identity whose bucket applies, or null for the default bucket.
     */
    String resolve(ClientQuotaType quotaType, KafkaPrincipal principal) {
        Map<String, Double> values = quotas.get(quotaType);
        if (values.isEmpty())
            return null;
        String best = null;
        double bestValue = 0;
        for (String identity : identitiesOf(principal)) {
            Double value = values.get(identity);
            if (value != null && (best == null || value > bestValue || (value == bestValue && identity.compareTo(best) < 0))) {
                best = identity;
                bestValue = value;
            }
        }
        return best;
    }

    /**
     * @return {@code Type:name} of the principal and, for a multi-identity principal, of each identity it carries.
     */
    static List<String> identitiesOf(KafkaPrincipal principal) {
        if (principal == null)
            return Collections.emptyList();
        List<String> identities = new ArrayList<>();
        identities.add(identityOf(principal));
        if (principal instanceof MultiIdentityPrincipal) {
            for (KafkaPrincipal p : ((MultiIdentityPrincipal) principal).aclPrincipals()) {
                String identity = identityOf(p);
                if (!identities.contains(identity))
                    identities.add(identity);
            }
        }
        return identities;
    }

    static String identityOf(KafkaPrincipal principal) {
        return principal.getPrincipalType() + ":" + principal.getName();
    }

    /**
     * @return The user part of a quota entity, or null for entities this callback ignores (client-id based).
     */
    private ClientQuotaEntity.ConfigEntity userEntity(ClientQuotaEntity quotaEntity) {
        List<ClientQuotaEntity.ConfigEntity> entities = quotaEntity.configEntities();
        if (entities.size() == 1) {
            ClientQuotaEntity.ConfigEntity entity = entities.get(0);
            if (entity.entityType() == ClientQuotaEntity.ConfigEntityType.USER
                    || entity.entityType() == ClientQuotaEntity.ConfigEntityType.DEFAULT_USER)
                return entity;
        }
        if (!clientIdQuotaWarned) {
            clientIdQuotaWarned = true;
            log.warn("Quotas keyed on client-id are ignored by {} (quota entity {}); quotas apply by security identity",
                MultiIdentityQuotaCallback.class.getSimpleName(), quotaEntity.configEntities());
        }
        return null;
    }

    private static String describe(ClientQuotaEntity.ConfigEntity user) {
        return user.entityType() == ClientQuotaEntity.ConfigEntityType.DEFAULT_USER ? "default identity" : user.name();
    }
}
