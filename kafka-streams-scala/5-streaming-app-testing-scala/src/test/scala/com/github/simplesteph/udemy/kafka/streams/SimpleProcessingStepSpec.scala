package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.junit.Assert.assertNull
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}


class SimpleProcessingStepSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  private var config: Properties = _

  private var builder: StreamsBuilder = _

  private var testDriver: TopologyTestDriver = _

  private val stringSerializer: Serializer[String] = Serdes.String.serializer

  private val recordFactory = new ConsumerRecordFactory[String, String](stringSerializer, stringSerializer)

  override def beforeEach(): Unit = {
    builder = new StreamsBuilder
    config = new Properties

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  }

  override def afterEach(): Unit = testDriver.close()

  "formatKeyValue" should "parse key as int and split the value" in {
      builder
        .stream("input-topic")(Consumed.`with`(Serdes.String, Serdes.String))
        .map(SimpleProcessingStep.formatKeyValue)
        .to("output-topic")(Produced.`with`(Serdes.Integer, Serdes.String))

      testDriver = new TopologyTestDriver(builder.build(), config)

      testDriver.pipeInput(recordFactory.create("input-topic", "0000101", "terminal-8,ryu"))

      val output = testDriver.readOutput("output-topic", Serdes.Integer.deserializer(), Serdes.String.deserializer())

      OutputVerifier.compareKeyValue(output, 101, "ryu")
    }

  "getMachineIdTest" should "extract the terminal number" in {
    builder
      .stream("input-topic")(Consumed.`with`(Serdes.String, Serdes.String))
      .mapValues(SimpleProcessingStep.getMachineId)
      .to("output-topic")(Produced.`with`(Serdes.String, Serdes.String))

    testDriver = new TopologyTestDriver(builder.build(), config)

    testDriver.pipeInput(recordFactory.create("input-topic", "notused", "terminal-9,ryu"))

    val output = testDriver.readOutput("output-topic", Serdes.String.deserializer(), Serdes.String.deserializer())

    OutputVerifier.compareKeyValue(output, "notused", "9")
  }

  "isTopCharacterTest" should "keep only the most famous character defined" in {
    builder
      .stream("input-topic")(Consumed.`with`(Serdes.String, Serdes.String))
      .filter(SimpleProcessingStep.isTopCharacter)
      .to("output-topic")(Produced.`with`(Serdes.String, Serdes.String))

    testDriver = new TopologyTestDriver(builder.build(), config)

    testDriver.pipeInput(recordFactory.create("input-topic", "0000101", "ryu"))
    testDriver.pipeInput(recordFactory.create("input-topic", "0000102", "jin"))
    testDriver.pipeInput(recordFactory.create("input-topic", "0000103", "blair"))
    testDriver.pipeInput(recordFactory.create("input-topic", "0000104", "chunli"))

    val output1 = testDriver.readOutput("output-topic", Serdes.String.deserializer, Serdes.String.deserializer)
    val output2 = testDriver.readOutput("output-topic", Serdes.String.deserializer, Serdes.String.deserializer)
    val output3 = testDriver.readOutput("output-topic", Serdes.String.deserializer, Serdes.String.deserializer)
    val output4 = testDriver.readOutput("output-topic", Serdes.String.deserializer, Serdes.String.deserializer)

    OutputVerifier.compareKeyValue(output1, "0000101", "ryu")
    OutputVerifier.compareKeyValue(output2, "0000104", "chunli")

    assertNull(output3)
    assertNull(output4)
  }
}
