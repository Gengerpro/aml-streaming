package com.aml.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AppConfigSpec extends AnyFlatSpec with Matchers {

  "AppConfig" should "load from default application.conf" in {
    val config = AppConfig.load()
    config.kafkaBootstrapServers should not be empty
    config.kafkaGroupId should not be empty
  }

  it should "load from custom resource" in {
    val config = AppConfig.load("test-config.conf")
    config.kafkaBootstrapServers shouldBe "localhost:9092"
  }
}
