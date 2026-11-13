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

import org.apache.kafka.common.security.auth.{KafkaPrincipal, MultiIdentityPrincipal}

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
 */
class MultiIdentityAclAuthorizer extends AclAuthorizer {

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
