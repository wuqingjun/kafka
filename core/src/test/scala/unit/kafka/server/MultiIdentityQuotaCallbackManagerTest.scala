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
package kafka.server

import java.net.InetAddress
import kafka.server.QuotaType._
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.security.auth.{KafkaPrincipal, MultiIdentityPrincipal}
import org.apache.kafka.common.utils.Sanitizer
import org.apache.kafka.network.Session
import org.apache.kafka.server.config.{ClientQuotaManagerConfig, ZooKeeperInternals}
import org.apache.kafka.server.quota.MultiIdentityQuotaCallback
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

/**
 * Kafka's own [[ClientQuotaManager]] (sliding-window rate, throttle time) with [[MultiIdentityQuotaCallback]]:
 * team members share their claim's budget, unknown principals each get the default, and client.id changes don't
 * escape the limit. Quotas are set the way Kafka passes stored `user` quotas (sanitized identity names).
 */
class MultiIdentityQuotaCallbackManagerTest extends BaseClientQuotaManagerTest {
  private val TeamRate = 1000.0
  private val DefaultRate = 100.0

  private def token(subject: String, claims: String*): KafkaPrincipal =
    new MultiIdentityPrincipal("User", subject, MultiIdentityPrincipal.Source.TOKEN, claims.asJava)

  private def session(principal: KafkaPrincipal) = new Session(principal, InetAddress.getLocalHost)

  private def withManager(test: ClientQuotaManager => Unit): Unit = {
    val manager = new ClientQuotaManager(new ClientQuotaManagerConfig(), metrics, Produce, time, "",
      Some(new MultiIdentityQuotaCallback))
    try {
      manager.updateQuota(Some(Sanitizer.sanitize("Claim:payments-team")), None, None, Some(new Quota(TeamRate, true)))
      manager.updateQuota(Some(ZooKeeperInternals.DEFAULT_STRING), None, None, Some(new Quota(DefaultRate, true)))
      test(manager)
    } finally {
      manager.shutdown()
    }
  }

  /** Each principal records `bytesPerSecond` for `seconds` seconds; returns the last throttle time of each. */
  private def produce(manager: ClientQuotaManager, seconds: Int, load: (KafkaPrincipal, String, Double)*): Seq[Int] = {
    var throttles = Seq.fill(load.size)(0)
    for (_ <- 0 until seconds) {
      throttles = load.map { case (principal, clientId, rate) =>
        manager.maybeRecordAndGetThrottleTimeMs(session(principal), clientId, rate, time.milliseconds)
      }
      time.sleep(1000)
    }
    throttles
  }

  @Test
  def testTeamMembersShareTheClaimBudget(): Unit = withManager { manager =>
    val alice = token("alice@example.com", "payments-team")
    val bob = token("bob@example.com", "payments-team")
    // 600 B/s each: each alone is under 1000 B/s, together they exceed the team's budget
    val throttles = produce(manager, 10, (alice, "alice-app", 600), (bob, "bob-app", 600))
    assertTrue(throttles.forall(_ > 0), s"both should be throttled by the shared budget: $throttles")
  }

  @Test
  def testOneTeamMemberAloneStaysUnderTheBudget(): Unit = withManager { manager =>
    val throttles = produce(manager, 10, (token("alice@example.com", "payments-team"), "alice-app", 600))
    assertEquals(Seq(0), throttles)
  }

  @Test
  def testDefaultIsPerPrincipal(): Unit = withManager { manager =>
    val noisy = token("noisy@example.com", "unknown-team")
    val quiet = new KafkaPrincipal("User", "O=Other,CN=quiet")
    val throttles = produce(manager, 10, (noisy, "n", 300), (quiet, "q", 50))
    assertTrue(throttles.head > 0, s"noisy is over the default: $throttles")
    assertEquals(0, throttles(1), "quiet has its own default bucket")
  }

  @Test
  def testChangingClientIdDoesNotEscape(): Unit = withManager { manager =>
    val alice = token("alice@example.com", "payments-team")
    var last = 0
    for (i <- 0 until 10) {
      // a new client.id every request, 1500 B/s in total
      last = manager.maybeRecordAndGetThrottleTimeMs(session(alice), s"client-$i", 1500, time.milliseconds)
      time.sleep(1000)
    }
    assertTrue(last > 0, "throttled despite changing client.id")
  }

  @Test
  def testRemovingTheClaimQuotaFallsBackToDefault(): Unit = withManager { manager =>
    val alice = token("alice@example.com", "payments-team")
    assertEquals(Seq(0), produce(manager, 5, (alice, "a", 600)))
    manager.updateQuota(Some(Sanitizer.sanitize("Claim:payments-team")), None, None, None)
    assertTrue(produce(manager, 10, (alice, "a", 600)).head > 0, "now limited by the default (100 B/s)")
  }
}
