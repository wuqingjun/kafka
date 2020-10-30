package com.microsoft.azpubsub.security.auth

import java.util
import java.util.{Timer, TimerTask}
import java.util.concurrent._

import scala.collection.JavaConverters.asScalaSetConverter
import com.yammer.metrics.core.{Meter, MetricName}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel.Session
import kafka.security.auth.{Operation, Resource, SimpleAclAuthorizer, Topic}
import kafka.utils.Logging
import kafka.utils.timer.{SystemTimer, TimerTask}

import scala.collection.mutable

/*
 * AzPubSub ACL Authorizer to handle the certificate & role based principal type
 */
class AzPubSubAclAuthorizer extends SimpleAclAuthorizer with Logging with KafkaMetricsGroup {
  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName("azpubsub.security", "AuthorizerMetrics", name, metricTags)
  }

  override def configure(javaConfigs: util.Map[String, _]): Unit = {
    super.configure(javaConfigs)
    config = AzPubSubConfig.fromProps(javaConfigs)
  }

  private var config: AzPubSubConfig = null
  private val successRate: Meter = newMeter("AuthorizerSuccessPerSec", "success", TimeUnit.SECONDS)
  private val failureRate: Meter = newMeter("AuthorizerFailurePerSec", "failure", TimeUnit.SECONDS)
  private val topicThreadCounterTimerTask  = new ThreadCounterTimerTask()
  private val trigger: Timer = new Timer(true)
  trigger.scheduleAtFixedRate(topicThreadCounterTimerTask, 5, 5000)

  var ints = new mutable.TreeSet[Long]
  override def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {

    if(config.getBoolean("azpubsub.enable.topic.qps.throttling") &&  resource.resourceType.equals( Topic )) {
      topicThreadCounterTimerTask.setTopicName(resource.name)
      topicThreadCounterTimerTask.setIoThreadId(Thread.currentThread().getId);

      val threadCount = Math.max(topicThreadCounterTimerTask.getThreadCount, 1)

      var count = 1
      while (ints.size * threadCount > config.getInt("azpubsub.topic.max.qps")) {
        val pivot = ints.minBy(x => x > System.currentTimeMillis - 1000)
        val (_, after) = ints.partition(x => x > pivot)
        ints = after
        Thread.sleep(count)
        count *= 2
      }

      ints += System.currentTimeMillis
    }

    val sessionPrincipal = session.principal
    if (classOf[AzPubSubPrincipal] == sessionPrincipal.getClass) {
      val principal = sessionPrincipal.asInstanceOf[AzPubSubPrincipal]
      for (role <- principal.getRoles.asScala) {
        val claimPrincipal = new KafkaPrincipal(principal.getPrincipalType(), role)
        val claimSession = new Session(claimPrincipal, session.clientAddress)
        if (super.authorize(claimSession, operation, resource)) {
          successRate.mark()
          return true
        }
      }
    } else if (super.authorize(session, operation, resource)) {
      successRate.mark()
      return true
    }

    failureRate.mark()
    return false
  }
}
