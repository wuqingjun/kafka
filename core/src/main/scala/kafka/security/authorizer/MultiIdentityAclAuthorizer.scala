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

import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.PolicyViolationException
import org.apache.kafka.common.security.auth.{KafkaPrincipal, MultiIdentityPrincipal}
import org.apache.kafka.common.security.authenticator.{DefaultSuperUsersValidator, SuperUsersValidationIssue, SuperUsersValidator}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.authorizer.{AclCreateResult, AclDeleteResult, AuthorizableRequestContext}

import java.util
import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.jdk.CollectionConverters._

/**
 * ZooKeeper-mode ACL authorizer that authorizes every identity of a [[MultiIdentityPrincipal]], e.g. the claims of
 * a SAML token. ACLs, storage (ZooKeeper) and all other behavior are those of [[AclAuthorizer]].
 *
 * For a MultiIdentityPrincipal the identities are its principal plus one per identity, e.g.
 * `User:alice@example.com`, `Claim:kafka-readers`, `Claim:payments-team`. A request is allowed if any identity has a
 * matching ALLOW ACL and none has a matching DENY ACL; it is a super user if any identity is in `super.users`.
 * Other principals are authorized exactly as by AclAuthorizer.
 *
 * Broker configuration:
 * {{{
 * authorizer.class.name=kafka.security.authorizer.MultiIdentityAclAuthorizer
 * principal.builder.class=org.apache.kafka.common.security.authenticator.MultiIdentityPrincipalBuilder
 * }}}
 * Without these settings (`authorizer.class.name=kafka.security.authorizer.AclAuthorizer`), the built-in authorizer
 * ignores the extra identities and only matches `User:<subject>`.
 *
 * At startup `super.users` is validated by a [[SuperUsersValidator]] (`authorizer.super.users.validation.class`,
 * default [[DefaultSuperUsersValidator]]). Every issue is logged; any ERROR stops the broker from starting.
 *
 * With `authorizer.acl.api.changes.enabled=false` (default `true`), ACLs can't be changed through the admin API:
 * `createAcls` / `deleteAcls` (e.g. `kafka-acls.sh --add/--remove`) fail with a [[PolicyViolationException]] for every
 * principal, super users included. `describeAcls` and authorization are unaffected. Use this when the ACLs in
 * ZooKeeper are managed from a deployed file (e.g. Cruise Control's ACL sync, which writes ZooKeeper directly).
 * Direct ZooKeeper writes are not blocked by this setting; they need ZooKeeper security.
 */
class MultiIdentityAclAuthorizer extends AclAuthorizer {
  import MultiIdentityAclAuthorizer._

  @volatile private var aclApiChangesEnabled = true

  override def configure(javaConfigs: util.Map[String, _]): Unit = {
    validateSuperUsers(javaConfigs)
    aclApiChangesEnabled = parseBoolean(javaConfigs, AclApiChangesEnabledProp, default = true)
    if (!aclApiChangesEnabled)
      info(s"$AclApiChangesEnabledProp=false: ACL changes through the admin API (createAcls / deleteAcls) are refused.")
    super.configure(javaConfigs)
  }

  override def createAcls(requestContext: AuthorizableRequestContext,
                          aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
    if (aclApiChangesEnabled)
      super.createAcls(requestContext, aclBindings)
    else {
      warn(s"Refused createAcls for ${aclBindings.size} binding(s) from ${requester(requestContext)}: $AclApiChangesEnabledProp=false")
      val result = new AclCreateResult(new PolicyViolationException(RefusedMessage))
      aclBindings.asScala.map(_ => CompletableFuture.completedFuture(result)).asJava
    }
  }

  override def deleteAcls(requestContext: AuthorizableRequestContext,
                          aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = {
    if (aclApiChangesEnabled)
      super.deleteAcls(requestContext, aclBindingFilters)
    else {
      warn(s"Refused deleteAcls for ${aclBindingFilters.size} filter(s) from ${requester(requestContext)}: $AclApiChangesEnabledProp=false")
      val result = new AclDeleteResult(new PolicyViolationException(RefusedMessage))
      aclBindingFilters.asScala.map(_ => CompletableFuture.completedFuture(result)).asJava
    }
  }

  /** The requesting principal for the log; the context is null when the authorizer runs outside a broker. */
  private def requester(requestContext: AuthorizableRequestContext): String =
    Option(requestContext).flatMap(c => Option(c.principal)).map(auditPrincipal).getOrElse("an unknown requester")

  private def validateSuperUsers(configs: util.Map[String, _]): Unit = {
    val className = Option(configs.get(SuperUsersValidator.VALIDATOR_CLASS_CONFIG)).map(_.toString.trim)
      .filter(_.nonEmpty).getOrElse(classOf[DefaultSuperUsersValidator].getName)
    val validator = try {
      Utils.newInstance(className, classOf[SuperUsersValidator])
    } catch {
      case _: ClassNotFoundException =>
        throw new ConfigException(SuperUsersValidator.VALIDATOR_CLASS_CONFIG, className, "class not found on the broker classpath")
    }
    validator.configure(configs)

    // Same split as AclAuthorizer.configure (a trailing ';' is ignored; an empty entry in the middle is reported)
    val entries = Option(configs.get(AclAuthorizer.SuperUsersProp)).map(_.toString).filter(_.nonEmpty)
      .map(_.split(";").map(_.trim).toList).getOrElse(Nil)
    val issues = validator.validate(entries.asJava).asScala.toList

    issues.foreach { issue =>
      issue.severity match {
        case SuperUsersValidationIssue.Severity.ERROR => error(issue.toString)
        case SuperUsersValidationIssue.Severity.WARNING => warn(issue.toString)
        case SuperUsersValidationIssue.Severity.INFO => info(issue.toString)
      }
    }
    val errors = issues.filter(_.severity == SuperUsersValidationIssue.Severity.ERROR)
    if (errors.nonEmpty)
      throw new ConfigException(s"Invalid ${AclAuthorizer.SuperUsersProp} (validated by $className): " +
        errors.map(_.toString).mkString(" | "))
  }

  private def parseBoolean(configs: util.Map[String, _], name: String, default: Boolean): Boolean =
    Option(configs.get(name)).map(_.toString.trim).filter(_.nonEmpty) match {
      case None => default
      case Some(v) if v.equalsIgnoreCase("true") => true
      case Some(v) if v.equalsIgnoreCase("false") => false
      case Some(v) => throw new ConfigException(name, v, "must be true or false")
    }

  override protected[authorizer] def aclPrincipals(sessionPrincipal: KafkaPrincipal): Set[KafkaPrincipal] =
    sessionPrincipal match {
      case p: MultiIdentityPrincipal => p.aclPrincipals().asScala.toSet
      case p => super.aclPrincipals(p)
    }

  /** e.g. "User:alice@example.com (TOKEN, 2 identities)": shows the source and count, never the claim values. */
  override protected[authorizer] def auditPrincipal(sessionPrincipal: KafkaPrincipal): String =
    sessionPrincipal match {
      case p: MultiIdentityPrincipal => p.describe()
      case p => super.auditPrincipal(p)
    }
}

object MultiIdentityAclAuthorizer {
  /** Whether ACLs may be created / deleted through the admin API (default true). */
  val AclApiChangesEnabledProp = "authorizer.acl.api.changes.enabled"

  private[authorizer] val RefusedMessage = "ACL changes through the admin API are disabled on this cluster " +
    s"($AclApiChangesEnabledProp=false); ACLs are managed from the deployed ACL file."
}
