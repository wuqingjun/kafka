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

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.security.auth.{KafkaPrincipal, MultiIdentityPrincipal}
import org.apache.kafka.common.security.authenticator.{DefaultSuperUsersValidator, SuperUsersValidationIssue, SuperUsersValidator}
import org.apache.kafka.common.utils.Utils

import java.util
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
 */
class MultiIdentityAclAuthorizer extends AclAuthorizer {

  override def configure(javaConfigs: util.Map[String, _]): Unit = {
    validateSuperUsers(javaConfigs)
    super.configure(javaConfigs)
  }

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
