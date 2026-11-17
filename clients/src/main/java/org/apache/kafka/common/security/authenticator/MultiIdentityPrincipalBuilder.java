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
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.MultiIdentityPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.saml.SamlOAuthBearerToken;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.security.sasl.SaslServer;

/**
 * Principal builder that adds identities from the authentication source to the principal, so they can be
 * authorized by ACLs (see {@link MultiIdentityPrincipal}).
 * <ul>
 *   <li><b>SAML token over SASL/OAUTHBEARER</b> (token validated by
 *       {@link org.apache.kafka.common.security.oauthbearer.saml.SamlOAuthBearerValidatorCallbackHandler}):
 *       {@code User:<subject>} with the token claims as identities ({@code Claim:<claim>} in ACLs).</li>
 *   <li><b>Everything else</b> (SSL certificates, PLAIN, SCRAM, other OAUTHBEARER tokens, plaintext): exactly what
 *       {@link DefaultKafkaPrincipalBuilder} returns, including {@code ssl.principal.mapping.rules}.</li>
 * </ul>
 * Broker configuration:
 * <pre>
 * principal.builder.class=org.apache.kafka.common.security.authenticator.MultiIdentityPrincipalBuilder
 * </pre>
 * Kerberos (GSSAPI) short-name rules are not supported by this builder.
 */
public class MultiIdentityPrincipalBuilder implements KafkaPrincipalBuilder, KafkaPrincipalSerde, Configurable {

    /** First byte of a serialized {@link MultiIdentityPrincipal}. The default serde starts with a 0 version byte. */
    static final byte MULTI_IDENTITY_MAGIC = 0x7F;

    private static final String OAUTHBEARER_TOKEN_PROPERTY = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM + ".token";

    private DefaultKafkaPrincipalBuilder delegate =
        new DefaultKafkaPrincipalBuilder(null, SslPrincipalMapper.fromRules("DEFAULT"));

    @Override
    public void configure(Map<String, ?> configs) {
        Object rules = configs.get(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG);
        SslPrincipalMapper sslPrincipalMapper = SslPrincipalMapper.fromRules(rules == null ? "DEFAULT" : rules.toString());
        delegate = new DefaultKafkaPrincipalBuilder(null, sslPrincipalMapper);
    }

    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        KafkaPrincipal principal = delegate.build(context);
        if (context instanceof SaslAuthenticationContext) {
            SaslServer server = ((SaslAuthenticationContext) context).server();
            if (OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(server.getMechanismName())) {
                Object token = server.getNegotiatedProperty(OAUTHBEARER_TOKEN_PROPERTY);
                if (token instanceof SamlOAuthBearerToken) {
                    List<String> claims = new ArrayList<>(((SamlOAuthBearerToken) token).scope());
                    return new MultiIdentityPrincipal(principal.getPrincipalType(), principal.getName(),
                        MultiIdentityPrincipal.Source.TOKEN, claims, principal.tokenAuthenticated());
                }
            }
        }
        return principal;
    }

    @Override
    public byte[] serialize(KafkaPrincipal principal) throws SerializationException {
        if (!(principal instanceof MultiIdentityPrincipal))
            return delegate.serialize(principal);
        MultiIdentityPrincipal p = (MultiIdentityPrincipal) principal;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(MULTI_IDENTITY_MAGIC);
            out.writeUTF(p.getPrincipalType());
            out.writeUTF(p.getName());
            out.writeBoolean(p.tokenAuthenticated());
            out.writeUTF(p.source().name());
            out.writeInt(p.identities().size());
            for (String identity : p.identities())
                out.writeUTF(identity);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Failed to serialize principal " + p.describe(), e);
        }
    }

    @Override
    public KafkaPrincipal deserialize(byte[] bytes) throws SerializationException {
        if (bytes.length == 0 || bytes[0] != MULTI_IDENTITY_MAGIC)
            return delegate.deserialize(bytes);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            in.readByte();
            String type = in.readUTF();
            String name = in.readUTF();
            boolean tokenAuthenticated = in.readBoolean();
            MultiIdentityPrincipal.Source source = MultiIdentityPrincipal.Source.valueOf(in.readUTF());
            int count = in.readInt();
            if (count < 0)
                throw new SerializationException("Negative identity count " + count);
            List<String> identities = new ArrayList<>(Math.min(count, 1024));
            for (int i = 0; i < count; i++)
                identities.add(in.readUTF());
            return new MultiIdentityPrincipal(type, name, source, identities, tokenAuthenticated);
        } catch (IOException | IllegalArgumentException e) {
            throw new SerializationException("Failed to deserialize multi-identity principal", e);
        }
    }
}
