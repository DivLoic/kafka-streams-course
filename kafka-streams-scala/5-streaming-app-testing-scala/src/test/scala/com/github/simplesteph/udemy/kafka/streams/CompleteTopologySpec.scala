package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.state.TimestampedKeyValueStore
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.junit.Assert.{assertEquals, assertNull}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}


class CompleteTopologySpec extends FlatSpec with Matchers with BeforeAndAfterEach {

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

  "createTopology" should "output the latest selection count for a character" in {
    testDriver = new TopologyTestDriver(CompleteTopology.createTopology(), config)

    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000101", "terminal-8,Ryu"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000102", "terminal-5,Jin"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000103", "terminal-1,Ryu"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000104", "terminal-3,Ken"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000105", "terminal-7,Ryu"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000106", "terminal-2,Chunli"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000107", "terminal-2,Alen"))

    val output1 = testDriver
      .readOutput("favourite-player-output-scala", Serdes.String.deserializer, Serdes.Long.deserializer)
    val output2 = testDriver
      .readOutput("favourite-player-output-scala", Serdes.String.deserializer, Serdes.Long.deserializer)
    val output3 = testDriver
      .readOutput("favourite-player-output-scala", Serdes.String.deserializer, Serdes.Long.deserializer)
    val output4 = testDriver
      .readOutput("favourite-player-output-scala", Serdes.String.deserializer, Serdes.Long.deserializer)
    val output5 = testDriver
      .readOutput("favourite-player-output-scala", Serdes.String.deserializer, Serdes.Long.deserializer)

    val output6 = testDriver.readOutput("favourite-player-output", Serdes.String.deserializer, Serdes.Long.deserializer)

    OutputVerifier.compareKeyValue(output1, "ryu", 1L)
    OutputVerifier.compareKeyValue(output2, "ryu", 2L)
    OutputVerifier.compareKeyValue(output3, "ken", 1L)
    OutputVerifier.compareKeyValue(output4, "ryu", 3L)
    OutputVerifier.compareKeyValue(output5, "chunli", 1L)

    assertNull(output6)
  }

  it should "keep a table with all character selected" in {
    testDriver = new TopologyTestDriver(CompleteTopology.createTopology(), config)

    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000101", "terminal-8,Ryu"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000102", "terminal-5,Jin"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000103", "terminal-1,Ryu"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000104", "terminal-3,Ken"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000105", "terminal-7,Ryu"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000106", "terminal-2,Chunli"))
    testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000107", "terminal-2,Alen"))

    val counts = testDriver.getKeyValueStore("CountsByPlayersScala").asInstanceOf[TimestampedKeyValueStore[String, Long]]

    assertEquals(3L, counts.get("ryu").value.longValue)

    assertEquals(1L, counts.get("ken").value.longValue)

    assertEquals(1L, counts.get("chunli").value.longValue)

    assertNull(counts.get("alen"))

    assertNull(counts.get("jin"))
  }

}
