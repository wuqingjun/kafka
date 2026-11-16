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
package kafka.security.authorizer

import kafka.server.{KafkaConfig, QuorumTestHarness}
import kafka.utils.TestUtils
import org.apache.kafka.common.acl.AclOperation.{ALTER_CONFIGS, DESCRIBE, DESCRIBE_CONFIGS, READ, WRITE}
import org.apache.kafka.common.acl.AclPermissionType.{ALLOW, DENY}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, GROUP, TOPIC}
import org.apache.kafka.common.resource.{Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, MultiIdentityPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.authenticator.{SuperUsersValidationIssue, SuperUsersValidator}
import org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import java.net.InetAddress
import java.util
import java.util.{Collections, UUID}
import scala.jdk.CollectionConverters._

/**
 * MultiIdentityAclAuthorizer with MultiIdentityPrincipal: ACLs on any identity (subject or claim) apply.
 * Also checks that the built-in AclAuthorizer keeps its single-principal behavior. ZooKeeper mode.
 */
class AclAuthorizerMultiIdentityTest extends QuorumTestHarness {

  private val host = InetAddress.getByName("192.168.0.1")
  private val adminContext = context(new KafkaPrincipal("User", "superuser"))
  private var authorizer: MultiIdentityAclAuthorizer = _
  private var builtIn: AclAuthorizer = _
  private var topic: ResourcePattern = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(AclAuthorizer.SuperUsersProp, "User:superuser;Claim:kafka-admins")
    val configs = KafkaConfig.fromProps(props).originals
    authorizer = new MultiIdentityAclAuthorizer
    authorizer.configure(configs)
    authorizer.maxUpdateRetries = Int.MaxValue
    builtIn = new AclAuthorizer
    builtIn.configure(configs)
    topic = new ResourcePattern(TOPIC, "orders-" + UUID.randomUUID(), LITERAL)
  }

  @AfterEach
  override def tearDown(): Unit = {
    authorizer.close()
    builtIn.close()
    TestUtils.clearYammerMetrics()
    super.tearDown()
  }

  private def token(subject: String, claims: String*): MultiIdentityPrincipal =
    new MultiIdentityPrincipal("User", subject, MultiIdentityPrincipal.Source.TOKEN, claims.asJava)

  private def context(principal: KafkaPrincipal): RequestContext = {
    val header = new RequestHeader(ApiKeys.PRODUCE, 2, "", 1)
    new RequestContext(header, "", host, principal, ListenerName.forSecurityProtocol(SecurityProtocol.SASL_SSL),
      SecurityProtocol.SASL_SSL, ClientInformation.EMPTY, false)
  }

  private def ace(principal: String, op: AclOperation, permission: org.apache.kafka.common.acl.AclPermissionType) =
    new AccessControlEntry(principal, WILDCARD_HOST, op, permission)

  private def addAcls(resource: ResourcePattern, aces: AccessControlEntry*): Unit = {
    authorizer.createAcls(adminContext, aces.map(new AclBinding(resource, _)).asJava).asScala
      .map(_.toCompletableFuture.get)
      .foreach(result => result.exception.ifPresent(e => throw e))
  }

  private def allowed(principal: KafkaPrincipal, op: AclOperation, resource: ResourcePattern): Boolean = {
    val action = new Action(op, resource, 1, true, true)
    authorizer.authorize(context(principal), Collections.singletonList(action)).get(0) == AuthorizationResult.ALLOWED
  }

  private def allowedByType(principal: KafkaPrincipal, op: AclOperation, resourceType: ResourceType): Boolean =
    authorizer.authorizeByResourceType(context(principal), op, resourceType) == AuthorizationResult.ALLOWED

  @Test
  def testClaimAclAllowsAnyTokenWithThatClaim(): Unit = {
    addAcls(topic, ace("Claim:kafka-readers", READ, ALLOW))

    assertTrue(allowed(token("alice@example.com", "payments-team", "kafka-readers"), READ, topic))
    assertTrue(allowed(token("bob@example.com", "kafka-readers"), READ, topic))
    assertFalse(allowed(token("carol@example.com", "payments-team"), READ, topic), "claim not present")
    assertFalse(allowed(token("alice@example.com", "kafka-readers"), WRITE, topic), "only READ granted")
  }

  @Test
  def testClaimTypeIsSeparateFromUserType(): Unit = {
    addAcls(topic, ace("Claim:kafka-readers", READ, ALLOW))
    // A user (or token subject) named like the claim does not match a Claim ACL
    assertFalse(allowed(new KafkaPrincipal("User", "kafka-readers"), READ, topic))
    assertFalse(allowed(token("kafka-readers"), READ, topic))
  }

  @Test
  def testSubjectAclStillWorks(): Unit = {
    addAcls(topic, ace("User:alice@example.com", WRITE, ALLOW))
    assertTrue(allowed(token("alice@example.com", "kafka-readers"), WRITE, topic))
    assertFalse(allowed(token("bob@example.com", "kafka-readers"), WRITE, topic))
  }

  @Test
  def testDenyOnAnyClaimWins(): Unit = {
    addAcls(topic, ace("Claim:kafka-readers", READ, ALLOW), ace("Claim:contractors", READ, DENY))
    assertTrue(allowed(token("alice@example.com", "kafka-readers"), READ, topic))
    assertFalse(allowed(token("dave@example.com", "kafka-readers", "contractors"), READ, topic))
  }

  @Test
  def testDenyOnSubjectWinsOverClaimAllow(): Unit = {
    addAcls(topic, ace("Claim:kafka-readers", READ, ALLOW), ace("User:mallory@example.com", READ, DENY))
    assertFalse(allowed(token("mallory@example.com", "kafka-readers"), READ, topic))
  }

  @Test
  def testReadOnClaimImpliesDescribe(): Unit = {
    addAcls(topic, ace("Claim:kafka-readers", READ, ALLOW))
    assertTrue(allowed(token("alice@example.com", "kafka-readers"), DESCRIBE, topic))
  }

  @Test
  def testPrefixedAndGroupAclsOnClaims(): Unit = {
    val prefix = "payments-" + UUID.randomUUID()
    addAcls(new ResourcePattern(TOPIC, prefix, PREFIXED), ace("Claim:payments-team", WRITE, ALLOW))
    addAcls(new ResourcePattern(GROUP, WILDCARD_RESOURCE, LITERAL), ace("Claim:payments-team", READ, ALLOW))

    val alice = token("alice@example.com", "payments-team")
    assertTrue(allowed(alice, WRITE, new ResourcePattern(TOPIC, prefix + "-events", LITERAL)))
    assertFalse(allowed(alice, WRITE, new ResourcePattern(TOPIC, "other-" + UUID.randomUUID(), LITERAL)))
    assertTrue(allowed(alice, READ, new ResourcePattern(GROUP, "any-group", LITERAL)))
  }

  @Test
  def testSuperUserByClaim(): Unit = {
    assertTrue(allowed(token("admin@example.com", "kafka-admins"), WRITE, topic))
    assertFalse(allowed(token("alice@example.com", "kafka-readers"), WRITE, topic))
  }

  @Test
  def testAuthorizeByResourceTypeUsesClaims(): Unit = {
    // Idempotent producers are authorized as "WRITE on any topic"
    addAcls(topic, ace("Claim:kafka-writers", WRITE, ALLOW))
    assertTrue(allowedByType(token("alice@example.com", "kafka-writers"), WRITE, TOPIC))
    assertFalse(allowedByType(token("bob@example.com", "kafka-readers"), WRITE, TOPIC))

    // A wildcard DENY on another claim of the same token wins
    addAcls(new ResourcePattern(TOPIC, WILDCARD_RESOURCE, LITERAL), ace("Claim:blocked", WRITE, DENY))
    assertFalse(allowedByType(token("eve@example.com", "kafka-writers", "blocked"), WRITE, TOPIC))
    assertTrue(allowedByType(token("alice@example.com", "kafka-writers"), WRITE, TOPIC))
  }

  @Test
  def testSuperUserByClaimForResourceType(): Unit = {
    assertTrue(allowedByType(token("admin@example.com", "kafka-admins"), WRITE, TOPIC))
  }

  @Test
  def testPlainPrincipalsUnchanged(): Unit = {
    addAcls(topic, ace("User:bob", READ, ALLOW))
    assertTrue(allowed(new KafkaPrincipal("User", "bob"), READ, topic))
    assertFalse(allowed(new KafkaPrincipal("User", "carol"), READ, topic))
    assertTrue(allowed(new KafkaPrincipal("User", "superuser"), WRITE, topic))
  }

  @Test
  def testAclPrincipals(): Unit = {
    assertEquals(Set(new KafkaPrincipal("User", "alice"), new KafkaPrincipal("Claim", "a"), new KafkaPrincipal("Claim", "b")),
      authorizer.aclPrincipals(token("alice", "a", "b")))
    assertEquals(Set(new KafkaPrincipal("User", "bob")), authorizer.aclPrincipals(new KafkaPrincipal("User", "bob")))
    // built-in: only type:name, as before
    assertEquals(Set(new KafkaPrincipal("User", "alice")), builtIn.aclPrincipals(token("alice", "a", "b")))
  }

  @Test
  def testAuditPrincipal(): Unit = {
    assertEquals("User:alice (TOKEN, 2 identities)", authorizer.auditPrincipal(token("alice", "a", "b")))
    assertEquals("User:alice", builtIn.auditPrincipal(token("alice", "a", "b")))
  }

  /** Configures a new MultiIdentityAclAuthorizer with extra broker settings; returns the exception, if any. */
  private def configureWith(extra: Map[String, String]): Option[Throwable] = {
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    extra.foreach { case (k, v) => props.put(k, v) }
    val a = new MultiIdentityAclAuthorizer
    try {
      a.configure(KafkaConfig.fromProps(props).originals)
      None
    } catch {
      case e: Throwable => Some(e)
    } finally {
      a.close()
    }
  }

  @Test
  def testInvalidSuperUsersStopsStartup(): Unit = {
    val e = configureWith(Map(AclAuthorizer.SuperUsersProp -> "User:superuser;User:CN=broker1, O=LocalDev"))
    assertTrue(e.exists(_.isInstanceOf[ConfigException]), s"expected ConfigException, got $e")
    assertTrue(e.get.getMessage.contains("Use: User:CN=broker1,O=LocalDev"), e.get.getMessage)
  }

  @Test
  def testWarningsDoNotStopStartup(): Unit = {
    // Claim entry and unknown type are warnings only
    assertEquals(None, configureWith(Map(AclAuthorizer.SuperUsersProp -> "User:superuser;Claim:kafka-admins;Uesr:bob")))
  }

  @Test
  def testCustomValidatorClass(): Unit = {
    val bad = Map(AclAuthorizer.SuperUsersProp -> "User:CN=broker1, O=LocalDev")
    assertEquals(None, configureWith(bad + (SuperUsersValidator.VALIDATOR_CLASS_CONFIG -> classOf[AcceptAllSuperUsersValidator].getName)))

    val e = configureWith(Map(AclAuthorizer.SuperUsersProp -> "User:alice",
      SuperUsersValidator.VALIDATOR_CLASS_CONFIG -> classOf[RejectAllSuperUsersValidator].getName))
    assertTrue(e.exists(_.getMessage.contains("rejected by test validator")), s"got $e")
    assertTrue(RejectAllSuperUsersValidator.configured, "validator must be configured before validate")
  }

  @Test
  def testUnknownValidatorClass(): Unit = {
    val e = configureWith(Map(SuperUsersValidator.VALIDATOR_CLASS_CONFIG -> "com.example.DoesNotExist"))
    assertTrue(e.exists(_.isInstanceOf[ConfigException]), s"got $e")
  }

  /** A MultiIdentityAclAuthorizer with ACL changes through the admin API disabled (same ZooKeeper as `authorizer`). */
  private def lockedAuthorizer(): MultiIdentityAclAuthorizer = {
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(AclAuthorizer.SuperUsersProp, "User:superuser;Claim:kafka-admins")
    props.put(MultiIdentityAclAuthorizer.AclApiChangesEnabledProp, "false")
    val a = new MultiIdentityAclAuthorizer
    a.configure(KafkaConfig.fromProps(props).originals)
    a
  }

  @Test
  def testAclApiChangesRefusedWhenDisabled(): Unit = {
    // ACL written while changes are allowed (as the deployed file would put it into ZooKeeper)
    addAcls(topic, ace("Claim:kafka-readers", READ, ALLOW))
    val locked = lockedAuthorizer()
    try {
      TestUtils.waitUntilTrue(() => locked.acls(new AclBindingFilter(topic.toFilter, AccessControlEntryFilter.ANY)).asScala.size == 1,
        "locked authorizer did not load the ACL")

      // create: refused for every binding, also for a super user (by name or by claim)
      for (requester <- Seq(adminContext, context(token("ops@example.com", "kafka-admins")))) {
        val bindings = Seq(new AclBinding(topic, ace("Claim:intruders", READ, ALLOW)),
          new AclBinding(topic, ace("User:mallory", WRITE, ALLOW))).asJava
        val results = locked.createAcls(requester, bindings).asScala.map(_.toCompletableFuture.get)
        assertEquals(2, results.size)
        results.foreach { r =>
          assertTrue(r.exception.isPresent && r.exception.get.isInstanceOf[PolicyViolationException], s"got ${r.exception}")
          assertTrue(r.exception.get.getMessage.contains(MultiIdentityAclAuthorizer.AclApiChangesEnabledProp))
        }
      }

      // delete: refused, the ACL stays
      val filter = new AclBindingFilter(topic.toFilter, AccessControlEntryFilter.ANY)
      val deleted = locked.deleteAcls(adminContext, Collections.singletonList(filter)).asScala.map(_.toCompletableFuture.get)
      assertEquals(1, deleted.size)
      assertTrue(deleted.head.exception.isPresent && deleted.head.exception.get.isInstanceOf[PolicyViolationException])

      // nothing changed in ZooKeeper (seen by the other instance), describe and authorize still work
      assertEquals(Set(ace("Claim:kafka-readers", READ, ALLOW)), authorizer.acls(filter).asScala.map(_.entry).toSet)
      assertEquals(Set(ace("Claim:kafka-readers", READ, ALLOW)), locked.acls(filter).asScala.map(_.entry).toSet)
      assertTrue(locked.authorize(context(token("bob@example.com", "kafka-readers")),
        Collections.singletonList(new Action(READ, topic, 1, true, true))).get(0) == AuthorizationResult.ALLOWED)
      assertFalse(locked.authorize(context(token("eve@example.com", "intruders")),
        Collections.singletonList(new Action(READ, topic, 1, true, true))).get(0) == AuthorizationResult.ALLOWED)
    } finally {
      locked.close()
    }
  }

  @Test
  def testAclApiChangesSettingValues(): Unit = {
    assertEquals(None, configureWith(Map(MultiIdentityAclAuthorizer.AclApiChangesEnabledProp -> "TRUE")))
    assertEquals(None, configureWith(Map(MultiIdentityAclAuthorizer.AclApiChangesEnabledProp -> " false ")))
    val e = configureWith(Map(MultiIdentityAclAuthorizer.AclApiChangesEnabledProp -> "yes"))
    assertTrue(e.exists(_.isInstanceOf[ConfigException]), s"got $e")
    // default (not set): changes allowed, as before
    addAcls(topic, ace("User:alice@example.com", WRITE, ALLOW))
    assertTrue(allowed(new KafkaPrincipal("User", "alice@example.com"), WRITE, topic))
  }

  private def contextFor(principal: KafkaPrincipal, apiKey: ApiKeys): RequestContext = {
    val header = new RequestHeader(apiKey, apiKey.latestVersion, "", 1)
    new RequestContext(header, "", host, principal, ListenerName.forSecurityProtocol(SecurityProtocol.SASL_SSL),
      SecurityProtocol.SASL_SSL, ClientInformation.EMPTY, false)
  }

  private def clusterAllowed(a: AclAuthorizer, principal: KafkaPrincipal, apiKey: ApiKeys, op: AclOperation): Boolean = {
    val action = new Action(op, new ResourcePattern(CLUSTER, Resource.CLUSTER_NAME, LITERAL), 1, true, true)
    a.authorize(contextFor(principal, apiKey), Collections.singletonList(action)).get(0) == AuthorizationResult.ALLOWED
  }

  @Test
  def testQuotaApiChangesOnlyForAllowedIdentities(): Unit = {
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(AclAuthorizer.SuperUsersProp, "User:superuser;Claim:kafka-admins;User:O=LocalDev,CN=cruise-control")
    props.put(MultiIdentityAclAuthorizer.QuotaApiChangesAllowedPrincipalsProp,
      " User:O=LocalDev,CN=cruise-control ; Claim:quota-admins ")
    val guarded = new MultiIdentityAclAuthorizer
    guarded.configure(KafkaConfig.fromProps(props).originals)
    try {
      val superUser = new KafkaPrincipal("User", "superuser")
      val cruiseControl = new KafkaPrincipal("User", "O=LocalDev,CN=cruise-control")
      // AlterClientQuotas: only the allowed identities, super users are refused
      assertFalse(clusterAllowed(guarded, superUser, ApiKeys.ALTER_CLIENT_QUOTAS, ALTER_CONFIGS))
      assertTrue(clusterAllowed(guarded, cruiseControl, ApiKeys.ALTER_CLIENT_QUOTAS, ALTER_CONFIGS))
      assertTrue(clusterAllowed(guarded, token("ops@example.com", "kafka-admins", "quota-admins"), ApiKeys.ALTER_CLIENT_QUOTAS, ALTER_CONFIGS))
      assertFalse(clusterAllowed(guarded, token("ops2@example.com", "kafka-admins"), ApiKeys.ALTER_CLIENT_QUOTAS, ALTER_CONFIGS))
      // an allowed identity still needs the permission itself
      assertFalse(clusterAllowed(guarded, token("x@example.com", "quota-admins"), ApiKeys.ALTER_CLIENT_QUOTAS, ALTER_CONFIGS))
      // other requests are not affected
      assertTrue(clusterAllowed(guarded, superUser, ApiKeys.INCREMENTAL_ALTER_CONFIGS, ALTER_CONFIGS))
      assertTrue(clusterAllowed(guarded, superUser, ApiKeys.DESCRIBE_CLIENT_QUOTAS, DESCRIBE_CONFIGS))
    } finally {
      guarded.close()
    }
  }

  @Test
  def testQuotaApiUnrestrictedByDefault(): Unit = {
    assertTrue(clusterAllowed(authorizer, new KafkaPrincipal("User", "superuser"), ApiKeys.ALTER_CLIENT_QUOTAS, ALTER_CONFIGS))
  }

  @Test
  def testInvalidQuotaApiPrincipal(): Unit = {
    val e = configureWith(Map(MultiIdentityAclAuthorizer.QuotaApiChangesAllowedPrincipalsProp -> "User:a;no-type"))
    assertTrue(e.exists(_.isInstanceOf[ConfigException]), s"got $e")
  }

  @Test
  def testBuiltInAclAuthorizerIgnoresClaims(): Unit = {
    addAcls(topic, ace("Claim:kafka-readers", READ, ALLOW), ace("User:alice@example.com", WRITE, ALLOW))
    // Wait until the built-in authorizer instance has seen the ACLs (shared ZooKeeper, separate cache)
    TestUtils.waitUntilTrue(() => builtIn.acls(new AclBindingFilter(topic.toFilter, AccessControlEntryFilter.ANY)).asScala.size == 2,
      "built-in authorizer did not receive the ACLs")
    val alice = token("alice@example.com", "kafka-readers")
    val ctx = context(alice)
    def builtInAllowed(op: AclOperation) =
      builtIn.authorize(ctx, Collections.singletonList(new Action(op, topic, 1, true, true))).get(0) == AuthorizationResult.ALLOWED

    assertFalse(builtInAllowed(READ), "built-in AclAuthorizer must not use claims")
    assertTrue(builtInAllowed(WRITE), "built-in AclAuthorizer still matches the subject")
    assertFalse(builtIn.authorizeByResourceType(context(token("x", "kafka-admins")), WRITE, TOPIC) == AuthorizationResult.ALLOWED,
      "built-in AclAuthorizer must not treat Claim:kafka-admins as super user")
  }
}

/** Test validator: accepts anything. */
class AcceptAllSuperUsersValidator extends SuperUsersValidator {
  override def configure(configs: util.Map[String, _]): Unit = {}
  override def validate(superUsers: util.List[String]): util.List[SuperUsersValidationIssue] = Collections.emptyList()
}

/** Test validator: reports one ERROR. */
class RejectAllSuperUsersValidator extends SuperUsersValidator {
  override def configure(configs: util.Map[String, _]): Unit = RejectAllSuperUsersValidator.configured = true
  override def validate(superUsers: util.List[String]): util.List[SuperUsersValidationIssue] =
    Collections.singletonList(new SuperUsersValidationIssue(SuperUsersValidationIssue.Severity.ERROR, null,
      "rejected by test validator", null))
}

object RejectAllSuperUsersValidator {
  @volatile var configured = false
}
