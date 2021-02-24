package com.github.mmolimar.kukulcan

import _root_.com.github.mmolimar.kukulcan.{KAdmin => SKAdmin}

import com.github.mmolimar.kukulcan.java.{KAdmin => JKAdmin}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal

import _root_.java.util.Collections._
import _root_.java.util.Properties

class KAdminAclsSpec extends KukulcanApiTestHarness with EmbeddedKafka {

  lazy implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

  override def apiClass: Class[_] = classOf[SKAdmin]

  override def execScalaTests(): Unit = withRunningKafka {
    val scalaApi: SKAdmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("zookeeper.connect", s"localhost:${config.zooKeeperPort}")
      SKAdmin(props)
    }

    scalaApi.servers shouldBe s"localhost:${config.kafkaPort}"
    scalaApi.client.isInstanceOf[KafkaAdminClient] shouldBe true

    scalaApi.acls.getAcls().isEmpty shouldBe true

    val resourcePattern = new ResourcePattern(ResourceType.TOPIC, "test", PatternType.LITERAL)
    val accessControl =
      new AccessControlEntry("User:test", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
    val acl = new AclBinding(resourcePattern, accessControl)
    scalaApi.acls.addAcls(Seq(acl))

    scalaApi.acls.getAcls().head.pattern().name() shouldBe "test"
    scalaApi.acls.listAcls()
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test")
    scalaApi.acls.listAcls(principals = Seq(principal))

    val aclFilter = new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY)
    scalaApi.acls.removeAcls(Seq(aclFilter))
  }

  override def execJavaTests(): Unit = withRunningKafka {
    val javaApi: JKAdmin = {
      val props = new Properties()
      props.put("bootstrap.servers", s"localhost:${config.kafkaPort}")
      props.put("zookeeper.connect", s"localhost:${config.zooKeeperPort}")
      new JKAdmin(props)
    }

    javaApi.servers shouldBe s"localhost:${config.kafkaPort}"
    javaApi.client.isInstanceOf[KafkaAdminClient] shouldBe true

    javaApi.acls.getAcls().isEmpty shouldBe true

    val resourcePattern = new ResourcePattern(ResourceType.TOPIC, "test", PatternType.LITERAL)
    val accessControl =
      new AccessControlEntry("User:test", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
    val acl = new AclBinding(resourcePattern, accessControl)
    javaApi.acls.addAcls(singletonList(acl))
    javaApi.acls.addAcls(singletonList(acl), true)

    javaApi.acls.getAcls().get(0).pattern().name() shouldBe "test"
    javaApi.acls.getAcls(AclBindingFilter.ANY, true).get(0).pattern().name() shouldBe "test"
    javaApi.acls.listAcls()
    javaApi.acls.listAcls(AclBindingFilter.ANY)
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test")
    javaApi.acls.listAcls(singletonList(principal))

    val aclFilter = new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY)
    javaApi.acls.removeAcls(singletonList(aclFilter))
    javaApi.acls.removeAcls(singletonList(aclFilter), true)

    javaApi.acls.defaultAuthorizer().isPresent shouldBe true
  }

}
