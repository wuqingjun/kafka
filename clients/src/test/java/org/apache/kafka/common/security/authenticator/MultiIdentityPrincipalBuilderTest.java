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

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.MultiIdentityPrincipal;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.saml.SamlOAuthBearerToken;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import javax.security.sasl.SaslServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiIdentityPrincipalBuilderTest {

    private static MultiIdentityPrincipalBuilder builder(String sslRules) {
        MultiIdentityPrincipalBuilder builder = new MultiIdentityPrincipalBuilder();
        builder.configure(sslRules == null ? Collections.emptyMap()
            : Collections.singletonMap(BrokerSecurityConfigs.SSL_PRINCIPAL_MAPPING_RULES_CONFIG, sslRules));
        return builder;
    }

    private static SaslAuthenticationContext oauthContext(Object token, String authorizationId) throws Exception {
        SaslServer server = mock(SaslServer.class);
        when(server.getMechanismName()).thenReturn("OAUTHBEARER");
        when(server.getAuthorizationID()).thenReturn(authorizationId);
        when(server.getNegotiatedProperty("OAUTHBEARER.token")).thenReturn(token);
        return new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL, InetAddress.getLocalHost(), "SASL_SSL");
    }

    @Test
    public void testSamlTokenGivesMultiIdentityPrincipalWithClaims() throws Exception {
        SamlOAuthBearerToken token = new SamlOAuthBearerToken("dG9rZW4=", "alice@example.com",
            Arrays.asList("kafka-readers", "payments-team"), Long.MAX_VALUE, 0L);

        KafkaPrincipal principal = builder(null).build(oauthContext(token, "alice@example.com"));

        MultiIdentityPrincipal p = assertInstanceOf(MultiIdentityPrincipal.class, principal);
        assertEquals("User", p.getPrincipalType());
        assertEquals("alice@example.com", p.getName());
        assertEquals(MultiIdentityPrincipal.Source.TOKEN, p.source());
        assertEquals(Arrays.asList("kafka-readers", "payments-team"), p.identities());
    }

    @Test
    public void testOtherOAuthBearerTokenGivesPlainPrincipal() throws Exception {
        OAuthBearerToken jwt = mock(OAuthBearerToken.class);
        when(jwt.scope()).thenReturn(Collections.singleton("scope1"));

        KafkaPrincipal principal = builder(null).build(oauthContext(jwt, "bob"));

        assertEquals(KafkaPrincipal.class, principal.getClass());
        assertEquals(new KafkaPrincipal("User", "bob"), principal);
    }

    @Test
    public void testPlainSaslGivesPlainPrincipal() throws Exception {
        SaslServer server = mock(SaslServer.class);
        when(server.getMechanismName()).thenReturn("PLAIN");
        when(server.getAuthorizationID()).thenReturn("carol");

        KafkaPrincipal principal = builder(null).build(
            new SaslAuthenticationContext(server, SecurityProtocol.SASL_SSL, InetAddress.getLocalHost(), "SASL_SSL"));

        assertEquals(new KafkaPrincipal("User", "carol"), principal);
        assertEquals(KafkaPrincipal.class, principal.getClass());
    }

    @Test
    public void testSslUsesMappingRules() throws Exception {
        SSLSession session = mock(SSLSession.class);
        when(session.getPeerPrincipal()).thenReturn(new X500Principal("CN=admin-client,O=LocalDev"));
        SslAuthenticationContext context = new SslAuthenticationContext(session, InetAddress.getLocalHost(), "SSL");

        assertEquals(new KafkaPrincipal("User", "CN=admin-client,O=LocalDev"), builder(null).build(context));
        assertEquals(new KafkaPrincipal("User", "admin-client"),
            builder("RULE:^CN=([^,]+),.*$/$1/,DEFAULT").build(context));
    }

    @Test
    public void testSerdeRoundTripKeepsIdentities() {
        MultiIdentityPrincipalBuilder builder = builder(null);
        MultiIdentityPrincipal original = new MultiIdentityPrincipal("User", "alice@example.com",
            MultiIdentityPrincipal.Source.TOKEN, Arrays.asList("kafka-readers", "urn:example:kafka:ops"), true);

        KafkaPrincipal copy = builder.deserialize(builder.serialize(original));

        MultiIdentityPrincipal p = assertInstanceOf(MultiIdentityPrincipal.class, copy);
        assertEquals(original, p);
        assertEquals(true, p.tokenAuthenticated());
        Set<KafkaPrincipal> expected = new HashSet<>(original.aclPrincipals());
        assertEquals(expected, new HashSet<>(p.aclPrincipals()));
    }

    @Test
    public void testSerdeRoundTripPlainPrincipalUsesDefaultFormat() {
        MultiIdentityPrincipalBuilder builder = builder(null);
        KafkaPrincipal plain = new KafkaPrincipal("User", "bob");
        byte[] bytes = builder.serialize(plain);

        assertEquals(plain, builder.deserialize(bytes));
        // same bytes as Kafka's default serde, so either side can read them
        assertEquals(plain, new DefaultKafkaPrincipalBuilder(null, null).deserialize(bytes));
    }
}
