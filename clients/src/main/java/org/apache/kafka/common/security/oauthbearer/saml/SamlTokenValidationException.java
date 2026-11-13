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

/**
 * Thrown by {@link SamlTokenValidator#validate(String)} when a token is not valid. The message should explain
 * why (it is logged by the broker, not sent to the client). Checked, so implementations and callers must handle
 * the invalid case explicitly.
 */
public class SamlTokenValidationException extends Exception {

    private static final long serialVersionUID = 1L;

    public SamlTokenValidationException(String message) {
        super(message);
    }

    public SamlTokenValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
