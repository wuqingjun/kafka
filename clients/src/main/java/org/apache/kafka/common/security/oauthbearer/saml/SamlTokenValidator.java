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

import org.apache.kafka.common.Configurable;

import java.io.Closeable;
import java.util.Map;

/**
 * Pluggable validator for SAML tokens that clients send over SASL/OAUTHBEARER.
 * <p>
 * The client base64-encodes its SAML token and sends it as the OAUTHBEARER token value. The broker passes
 * that string to {@link #validate(String)}. Each enterprise provides its own implementation (decoding,
 * signature and condition checks, claim extraction); Kafka only depends on this interface.
 *
 * <h3>Broker configuration</h3>
 * <pre>
 * saml.token.validator.class=com.example.MySamlTokenValidator
 * saml.token.validator.&lt;name&gt;=&lt;value&gt;     # any number of implementation-specific settings
 * </pre>
 * The value of {@value #VALIDATOR_CLASS_CONFIG} is a fully qualified class name. The class (and its
 * dependencies) must be on the broker classpath, e.g. a jar added via the {@code CLASSPATH} environment
 * variable. It must be public with a public no-argument constructor.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>The broker creates one instance per listener that uses SAML tokens.</li>
 *   <li>{@link #configure(Map)} is called once, with every broker setting that starts with
 *       {@value #VALIDATOR_CONFIG_PREFIX}, with that prefix removed (e.g. {@code saml.token.validator.audience}
 *       arrives as {@code audience}). Values are the raw strings from the properties file.</li>
 *   <li>{@link #validate(String)} is called for every client authentication and re-authentication, from
 *       multiple network threads at the same time. <b>Implementations must be thread-safe.</b></li>
 *   <li>{@link #close()} is called when the broker shuts down.</li>
 * </ol>
 */
public interface SamlTokenValidator extends Configurable, Closeable {

    /** Broker setting naming the implementation class. */
    String VALIDATOR_CLASS_CONFIG = "saml.token.validator.class";

    /** Prefix of all settings passed to {@link #configure(Map)} (prefix removed). */
    String VALIDATOR_CONFIG_PREFIX = "saml.token.validator.";

    /**
     * Receives the implementation's settings. Default: no settings needed.
     *
     * @param configs broker settings starting with {@value #VALIDATOR_CONFIG_PREFIX}, prefix removed
     */
    @Override
    default void configure(Map<String, ?> configs) {
    }

    /**
     * Validates a SAML token.
     *
     * @param base64Token the token exactly as the client sent it: the SAML token, base64-encoded
     * @return the validation result; returned <b>only</b> when the token is valid
     * @throws SamlTokenValidationException if the token is invalid for any reason (cannot be decoded, malformed,
     *         expired, untrusted, missing subject, ...). The message should say why; it is logged by the broker
     *         and not sent to the client. Authentication of that client fails.
     */
    SamlTokenValidationResult validate(String base64Token) throws SamlTokenValidationException;

    /** Releases resources. Default: nothing to release. */
    @Override
    default void close() {
    }
}
