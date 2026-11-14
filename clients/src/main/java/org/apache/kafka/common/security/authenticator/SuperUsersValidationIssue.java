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
package org.apache.kafka.common.security.authenticator;

import java.util.Objects;
import java.util.Optional;

/** One problem found by a {@link SuperUsersValidator}. */
public final class SuperUsersValidationIssue {

    public enum Severity {
        /** The broker does not start. */
        ERROR,
        /** Logged at WARN; the broker starts. */
        WARNING,
        /** Logged at INFO; the broker starts. */
        INFO
    }

    private final Severity severity;
    private final String entry;
    private final String message;
    private final String suggestion;

    /**
     * @param severity   how serious the problem is
     * @param entry      the {@code super.users} entry concerned, or null if the issue is about the setting as a whole
     * @param message    what is wrong and its effect
     * @param suggestion the corrected entry to use, or null if there is none
     */
    public SuperUsersValidationIssue(Severity severity, String entry, String message, String suggestion) {
        this.severity = Objects.requireNonNull(severity, "severity");
        this.entry = entry;
        this.message = Objects.requireNonNull(message, "message");
        this.suggestion = suggestion;
    }

    public Severity severity() {
        return severity;
    }

    public Optional<String> entry() {
        return Optional.ofNullable(entry);
    }

    public String message() {
        return message;
    }

    public Optional<String> suggestion() {
        return Optional.ofNullable(suggestion);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (entry != null)
            sb.append("super.users entry '").append(entry).append("': ");
        else
            sb.append("super.users: ");
        sb.append(message);
        if (suggestion != null)
            sb.append(" Use: ").append(suggestion);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SuperUsersValidationIssue that = (SuperUsersValidationIssue) o;
        return severity == that.severity && Objects.equals(entry, that.entry) && message.equals(that.message)
            && Objects.equals(suggestion, that.suggestion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(severity, entry, message, suggestion);
    }
}
