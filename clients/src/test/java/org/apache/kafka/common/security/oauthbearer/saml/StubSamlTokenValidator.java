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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Test validator: decides by token value. Records its configs and whether it was closed in static fields
 * (tests that inspect them reset them first).
 */
public class StubSamlTokenValidator implements SamlTokenValidator {

    /** Standard-base64 characters + / = included, to exercise Kafka's OAUTHBEARER token pattern. */
    public static final String VALID_TOKEN = "PHNhbWw+QUJD/xyz+w==";
    public static final String NO_EXPIRY_TOKEN = "bm8tZXhwaXJ5";
    public static final String EXPIRED_TOKEN = "ZXhwaXJlZA==";
    public static final String RUNTIME_ERROR_TOKEN = "Ym9vbQ==";
    public static final String NULL_RESULT_TOKEN = "bnVsbA==";

    public static final long NOW_MS = 1_800_000_000_000L;
    public static final long EXPIRES_AT_MS = NOW_MS + 30 * 60 * 1000L;

    static volatile Map<String, ?> lastConfigs;
    static volatile boolean closed;

    static void reset() {
        lastConfigs = null;
        closed = false;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        lastConfigs = new HashMap<>(configs);
    }

    @Override
    public SamlTokenValidationResult validate(String base64Token) throws SamlTokenValidationException {
        switch (base64Token) {
            case VALID_TOKEN:
                return new SamlTokenValidationResult("alice@example.com",
                    Arrays.asList("kafka-readers", "payments-team", "kafka-readers"), EXPIRES_AT_MS);
            case NO_EXPIRY_TOKEN:
                return new SamlTokenValidationResult("bob@example.com", Arrays.asList("ops"), null);
            case EXPIRED_TOKEN:
                return new SamlTokenValidationResult("carol@example.com", Arrays.asList("ops"), NOW_MS - 1);
            case RUNTIME_ERROR_TOKEN:
                throw new IllegalStateException("validator bug");
            case NULL_RESULT_TOKEN:
                return null;
            default:
                throw new SamlTokenValidationException("unknown token");
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
