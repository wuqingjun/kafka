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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.Configurable;

import java.util.List;
import java.util.Map;

/**
 * Validates the {@code super.users} broker setting when the authorizer starts, so that mistakes (for example a
 * certificate DN in the wrong order) are found at startup instead of as denied requests at runtime.
 * <p>
 * Used by {@code kafka.security.authorizer.MultiIdentityAclAuthorizer} (not by the built-in {@code AclAuthorizer}).
 * The implementation is chosen with:
 * <pre>
 * authorizer.super.users.validation.class=com.example.MySuperUsersValidator   # default: DefaultSuperUsersValidator
 * </pre>
 * The class must be public with a public no-argument constructor and be on the broker classpath.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>{@link #configure(Map)} is called once with <b>all</b> broker settings (the same map the authorizer receives),
 *       so the validator can read listeners, keystores and {@code ssl.principal.mapping.rules}. Settings meant only for
 *       the validator should use the prefix {@value #CONFIG_PREFIX}.</li>
 *   <li>{@link #validate(List)} is called once with the {@code super.users} entries.</li>
 *   <li>The authorizer logs every returned issue. If any issue has severity {@link SuperUsersValidationIssue.Severity#ERROR},
 *       the authorizer throws a {@code ConfigException} listing all errors and <b>the broker does not start</b>.</li>
 * </ol>
 */
public interface SuperUsersValidator extends Configurable {

    /** Broker setting naming the validator class. */
    String VALIDATOR_CLASS_CONFIG = "authorizer.super.users.validation.class";

    /** Prefix for validator-specific settings. */
    String CONFIG_PREFIX = "authorizer.super.users.validation.";

    /**
     * @param configs all broker settings (raw values as in the properties file)
     */
    @Override
    void configure(Map<String, ?> configs);

    /**
     * Validates {@code super.users}.
     *
     * @param superUsers the entries of {@code super.users}: the value split on {@code ;} and trimmed, in configured order.
     *                   Empty entries (e.g. from {@code a;;b}) are passed as empty strings. Empty list if not set.
     * @return all problems found (empty if none). Invalid entries must be reported as issues, not thrown.
     */
    List<SuperUsersValidationIssue> validate(List<String> superUsers);
}
