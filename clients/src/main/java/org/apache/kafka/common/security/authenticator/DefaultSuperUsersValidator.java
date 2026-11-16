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

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.MultiIdentityPrincipal;
import org.apache.kafka.common.security.authenticator.SuperUsersValidationIssue.Severity;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.security.auth.x500.X500Principal;

/**
 * Default {@link SuperUsersValidator}, focused on certificate (X.509) principals. Errors stop the broker.
 *
 * <table>
 *   <caption>Checks</caption>
 *   <tr><th>Check</th><th>Severity</th></tr>
 *   <tr><td>Entry empty, without {@code type:}, or with an empty name</td><td>ERROR</td></tr>
 *   <tr><td>DN not in the form Kafka produces (RFC 2253: no spaces after commas, upper-case attribute names),
 *       e.g. {@code User:CN=x, O=y}</td><td>ERROR</td></tr>
 *   <tr><td>DN that {@code ssl.principal.mapping.rules} maps to another name, so it can never match</td><td>ERROR</td></tr>
 *   <tr><td>This broker's own certificate DN listed in a different order than Kafka produces,
 *       e.g. {@code User:CN=broker1,O=Org} for a certificate that gives {@code User:O=Org,CN=broker1}</td><td>ERROR</td></tr>
 *   <tr><td>This broker's own certificate principal not listed at all (fine only if the brokers have ACLs)</td><td>WARNING</td></tr>
 *   <tr><td>Unknown principal type (typo), e.g. {@code Uesr:alice}</td><td>WARNING</td></tr>
 *   <tr><td>{@code Claim:} entry (every token with that claim gets full access)</td><td>WARNING</td></tr>
 *   <tr><td>Duplicate entry; own-certificate check skipped (with the reason)</td><td>INFO</td></tr>
 * </table>
 * The own-certificate check uses the inter-broker listener's keystore ({@code listener.name.<l>.ssl.keystore.*} or
 * {@code ssl.keystore.*}) and runs only if that listener is SSL with client authentication.
 * <p>
 * Settings: {@code authorizer.super.users.validation.principal.types} (comma-separated known principal types,
 * default {@code User,Claim,Certificate}).
 */
public class DefaultSuperUsersValidator implements SuperUsersValidator {

    public static final String PRINCIPAL_TYPES_CONFIG = CONFIG_PREFIX + "principal.types";
    public static final String DEFAULT_PRINCIPAL_TYPES = "User,Claim,Certificate";

    private Map<String, ?> configs = Collections.emptyMap();
    private Set<String> knownTypes = new HashSet<>();

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = new HashMap<>(configs);
        knownTypes = new HashSet<>();
        for (String type : string(PRINCIPAL_TYPES_CONFIG, DEFAULT_PRINCIPAL_TYPES).split(",")) {
            if (!type.trim().isEmpty())
                knownTypes.add(type.trim());
        }
    }

    @Override
    public List<SuperUsersValidationIssue> validate(List<String> superUsers) {
        List<SuperUsersValidationIssue> issues = new ArrayList<>();
        SslPrincipalMapper globalMapper = mapper(string("ssl.principal.mapping.rules", "DEFAULT"), issues);
        Set<String> seen = new LinkedHashSet<>();

        for (String entry : superUsers) {
            if (!seen.add(entry)) {
                issues.add(new SuperUsersValidationIssue(Severity.INFO, entry, "Duplicate entry.", null));
                continue;
            }
            checkEntry(entry, globalMapper, issues);
        }
        checkOwnCertificate(seen, issues);
        return issues;
    }

    private void checkEntry(String entry, SslPrincipalMapper mapper, List<SuperUsersValidationIssue> issues) {
        String[] typeAndName = checkSyntax(entry, issues);
        if (typeAndName == null)
            return;
        String type = typeAndName[0];
        String name = typeAndName[1];
        checkType(entry, type, issues);
        if ("User".equals(type) || MultiIdentityPrincipal.CERTIFICATE_TYPE.equals(type))
            checkDn(entry, type, name, mapper, issues);
    }

    /** Returns {type, name}, or null after reporting an ERROR. */
    private static String[] checkSyntax(String entry, List<SuperUsersValidationIssue> issues) {
        if (entry.isEmpty()) {
            issues.add(new SuperUsersValidationIssue(Severity.ERROR, entry,
                "Empty entry (e.g. ';;' or a trailing ';' followed by spaces).", null));
            return null;
        }
        int colon = entry.indexOf(':');
        if (colon <= 0) {
            issues.add(new SuperUsersValidationIssue(Severity.ERROR, entry,
                "Missing principal type; expected 'type:name', e.g. 'User:" + entry + "'.", "User:" + entry));
            return null;
        }
        String name = entry.substring(colon + 1);
        if (name.isEmpty()) {
            issues.add(new SuperUsersValidationIssue(Severity.ERROR, entry, "Empty principal name.", null));
            return null;
        }
        return new String[] {entry.substring(0, colon), name};
    }

    private void checkType(String entry, String type, List<SuperUsersValidationIssue> issues) {
        if (!knownTypes.contains(type)) {
            issues.add(new SuperUsersValidationIssue(Severity.WARNING, entry,
                "Unknown principal type '" + type + "' (known: " + knownTypes + "; types are case-sensitive). "
                    + "It will only match principals of exactly this type.", null));
        }
        if (MultiIdentityPrincipal.CLAIM_TYPE.equals(type)) {
            issues.add(new SuperUsersValidationIssue(Severity.WARNING, entry,
                "Every token with this claim gets full access, and DENY ACLs can't restrict it.", null));
        }
    }

    private static void checkDn(String entry, String type, String name, SslPrincipalMapper mapper,
                                List<SuperUsersValidationIssue> issues) {
        X500Principal dn = parseDn(name);
        if (dn == null)
            return; // not a certificate DN (e.g. a SASL user name or token subject)
        String normalized = dn.getName(X500Principal.RFC2253);
        if (!normalized.equals(name)) {
            issues.add(new SuperUsersValidationIssue(Severity.ERROR, entry,
                "Certificate DN is not in the form Kafka produces (RFC 2253: no spaces after commas, upper-case "
                    + "attribute names, escaped special characters), so it can never match.",
                type + ":" + normalized));
            return;
        }
        if (!"User".equals(type) || mapper == null)
            return;
        String mapped = map(mapper, normalized);
        if (mapped != null && !mapped.equals(normalized)) {
            issues.add(new SuperUsersValidationIssue(Severity.ERROR, entry,
                "ssl.principal.mapping.rules maps certificates with this DN to 'User:" + mapped
                    + "', so this entry can never match.", "User:" + mapped));
        }
    }

    /** The principal this broker's own certificate produces on the inter-broker listener must be a super user. */
    private void checkOwnCertificate(Set<String> entries, List<SuperUsersValidationIssue> issues) {
        String[] own = ownCertificatePrincipal(issues);
        if (own == null || entries.contains(own[1]))
            return;
        String location = own[0];
        String principal = own[1];
        String mapped = principal.substring("User:".length());
        for (String entry : entries) {
            if (entry.startsWith("User:") && sameRdnsInAnyOrder(entry.substring("User:".length()), mapped)) {
                issues.add(new SuperUsersValidationIssue(Severity.ERROR, entry,
                    "This broker's certificate (" + location + ") produces the principal '" + principal
                        + "': same DN, different order. Kafka prints DNs in RFC 2253 order (the reverse of "
                        + "openssl -subj). Inter-broker requests will be DENIED.", principal));
                return;
            }
        }
        issues.add(new SuperUsersValidationIssue(Severity.WARNING, null,
            "This broker's certificate (" + location + ") produces the principal '" + principal + "', which is not in "
                + "super.users. Inter-broker requests will be denied unless the brokers have ACLs.", principal));
    }

    /**
     * The principal Kafka builds for this broker's own certificate on the inter-broker listener, as
     * {keystore location, "User:<name>"}; null (after reporting why) if it can't be determined or doesn't apply.
     */
    private String[] ownCertificatePrincipal(List<SuperUsersValidationIssue> issues) {
        String listener = string("inter.broker.listener.name", string("security.inter.broker.protocol", "PLAINTEXT"));
        String prefix = "listener.name." + listener.toLowerCase(Locale.ROOT) + ".";
        String location = listenerString(prefix, "ssl.keystore.location", null);
        String type = listenerString(prefix, "ssl.keystore.type", "JKS");
        String reason = skipReason(listener, prefix, location, type);
        if (reason != null) {
            skip(issues, reason);
            return null;
        }
        String subject;
        try {
            subject = certificateSubject(location, type, listenerString(prefix, "ssl.keystore.password", null));
        } catch (Exception e) {
            issues.add(new SuperUsersValidationIssue(Severity.WARNING, null,
                "Could not read this broker's keystore " + location + " to check its certificate principal: " + e, null));
            return null;
        }
        if (subject == null) {
            skip(issues, "no private key entry with an X.509 certificate in " + location + ".");
            return null;
        }
        SslPrincipalMapper mapper = mapper(listenerString(prefix, "ssl.principal.mapping.rules", "DEFAULT"), issues);
        String mapped = mapper == null ? null : map(mapper, subject);
        return mapped == null ? null : new String[] {location, "User:" + mapped};
    }

    /** Why the own-certificate check doesn't apply, or null if it does. */
    private String skipReason(String listener, String prefix, String location, String keystoreType) {
        String protocol = protocolOf(listener);
        if (!"SSL".equalsIgnoreCase(protocol))
            return "inter-broker listener '" + listener + "' uses " + protocol + ", not SSL client certificates.";
        if ("none".equalsIgnoreCase(listenerString(prefix, "ssl.client.auth", "none")))
            return "ssl.client.auth=none on listener '" + listener + "' (brokers connect as User:ANONYMOUS).";
        if (location == null)
            return "no ssl.keystore.location for listener '" + listener + "'.";
        if ("PEM".equalsIgnoreCase(keystoreType))
            return "PEM keystores are not read by this validator.";
        return null;
    }

    private static void skip(List<SuperUsersValidationIssue> issues, String reason) {
        issues.add(new SuperUsersValidationIssue(Severity.INFO, null,
            "Broker certificate check skipped: " + reason, null));
    }

    private static String certificateSubject(String location, String type, String password) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(Paths.get(location))) {
            keyStore.load(in, password == null ? null : password.toCharArray());
        }
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!keyStore.isKeyEntry(alias))
                continue;
            Certificate certificate = keyStore.getCertificate(alias);
            if (certificate instanceof X509Certificate)
                return ((X509Certificate) certificate).getSubjectX500Principal().getName(); // RFC 2253, as Kafka uses
        }
        return null;
    }

    private String protocolOf(String listener) {
        String map = string("listener.security.protocol.map", null);
        if (map != null) {
            for (String pair : map.split(",")) {
                String[] kv = pair.trim().split(":", 2);
                if (kv.length == 2 && kv[0].trim().equalsIgnoreCase(listener))
                    return kv[1].trim();
            }
        }
        return listener;
    }

    private SslPrincipalMapper mapper(String rules, List<SuperUsersValidationIssue> issues) {
        try {
            return SslPrincipalMapper.fromRules(rules);
        } catch (IllegalArgumentException e) {
            issues.add(new SuperUsersValidationIssue(Severity.ERROR, null,
                "Invalid ssl.principal.mapping.rules '" + rules + "': " + e.getMessage(), null));
            return null;
        }
    }

    private static String map(SslPrincipalMapper mapper, String dn) {
        try {
            return mapper.getName(dn);
        } catch (Exception e) {
            return null;
        }
    }

    /** The name parsed as an X.500 DN, or null if it isn't one. */
    static X500Principal parseDn(String name) {
        if (name.indexOf('=') < 0)
            return null;
        try {
            return new X500Principal(name);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** True if both are DNs with the same RDNs, regardless of order (but not identical strings). */
    static boolean sameRdnsInAnyOrder(String a, String b) {
        List<String> ra = canonicalRdns(a);
        List<String> rb = canonicalRdns(b);
        return ra != null && rb != null && ra.size() > 1 && ra.equals(rb) && !a.equals(b);
    }

    private static List<String> canonicalRdns(String dn) {
        if (parseDn(dn) == null)
            return null;
        List<String> rdns = new ArrayList<>();
        for (String rdn : splitRdns(dn)) {
            X500Principal single = parseDn(rdn.trim());
            if (single == null)
                return null;
            rdns.add(single.getName(X500Principal.CANONICAL));
        }
        Collections.sort(rdns);
        return rdns;
    }

    /** Splits a DN at commas that are not escaped with '\' and not inside double quotes. */
    private static List<String> splitRdns(String dn) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        boolean quoted = false;
        for (char c : dn.toCharArray()) {
            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == '\\') {
                current.append(c);
                escaped = true;
            } else if (c == '"') {
                current.append(c);
                quoted = !quoted;
            } else if (c == ',' && !quoted) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        parts.add(current.toString());
        return parts;
    }

    private String listenerString(String listenerPrefix, String key, String defaultValue) {
        String value = string(listenerPrefix + key, null);
        return value != null ? value : string(key, defaultValue);
    }

    private String string(String key, String defaultValue) {
        Object value = configs.get(key);
        if (value == null)
            return defaultValue;
        String s = value instanceof Password ? ((Password) value).value() : value.toString();
        return s.trim().isEmpty() ? defaultValue : s.trim();
    }
}
