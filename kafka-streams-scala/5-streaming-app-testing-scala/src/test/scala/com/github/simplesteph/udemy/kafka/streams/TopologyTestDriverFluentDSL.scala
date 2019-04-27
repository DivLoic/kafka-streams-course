package com.github.simplesteph.udemy.kafka.streams

import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.scalatest.{FlatSpec, Matchers}


class TopologyTestDriverFluentDSL extends FlatSpec with Matchers {

  "formatKeyValue" should "parse key as int and split the value" in {
    val input = Seq(("0000101", "terminal-8,ryu"))

    val topology = MockedStreams().topology { builder =>
      builder
        .stream("input-topic")(Consumed.`with`(Serdes.String, Serdes.String))
        .map(SimpleProcessingStep.formatKeyValue)
        .to("output-topic")(Produced.`with`(Serdes.Integer, Serdes.String))
    }

    val flow = topology.input("input-topic", Serdes.String, Serdes.String, input)

    val result = flow.output("output-topic", Serdes.Integer, Serdes.String, 1)

    result should have length 1

    val Seq((keyResult, valueResult)) = result

    keyResult shouldBe 101
    valueResult shouldBe "ryu"
  }

  "getMachineIdTest" should "extract the terminal number" in {
    val input = Seq(("notused", "terminal-9,ryu"))

    val topology = MockedStreams().topology { builder =>
      builder
        .stream("input-topic")(Consumed.`with`(Serdes.String, Serdes.String))
        .mapValues(SimpleProcessingStep.getMachineId)
        .to("output-topic")(Produced.`with`(Serdes.String, Serdes.String))
    }

    val flow = topology.input("input-topic", Serdes.String, Serdes.String, input)

    val (_, valueResult) = flow.output("output-topic", Serdes.String, Serdes.String, 1).head

    valueResult shouldBe "9"
  }

  "isTopCharacterTest" should "keep only the most famous character defined" in {
    val inputs = Seq(("0000101", "ryu"),
      ("0000102", "jin"),
      ("0000103", "blair"),
      ("0000104", "chunli")
    )

    val topology = MockedStreams().topology { builder =>
      builder
        .stream("input-topic")(Consumed.`with`(Serdes.String, Serdes.String))
        .filter(SimpleProcessingStep.isTopCharacter)
        .to("output-topic")(Produced.`with`(Serdes.String, Serdes.String))
    }

    val flow = topology.input("input-topic", Serdes.String, Serdes.String, inputs)

    val result = flow.output("output-topic", Serdes.String, Serdes.String, 4)

    result should have length 2

    result should contain allElementsOf Seq(("0000101", "ryu"), ("0000104", "chunli"))
  }

  "createTopology" should "output the latest selection count for a character" in {
    val inputs = Seq(("0000101", "terminal-8,Ryu"),
      ("0000102", "terminal-5,Jin"),
      ("0000103", "terminal-1,Ryu"),
      ("0000104", "terminal-3,Ken"),
      ("0000105", "terminal-7,Ryu"),
      ("0000106", "terminal-2,Chunli"),
      ("0000107", "terminal-2,Alen"))

    val topology = MockedStreams().topology(CompleteTopology.createTopology)

    val flow = topology.input("favourite-player-input", Serdes.String, Serdes.String, inputs)

    val result = flow.output("favourite-player-output-scala", Serdes.String, Serdes.Long, 6)

    result should have length 5

    result should contain theSameElementsInOrderAs Seq(
      ("ryu", 1L),
      ("ryu", 2L),
      ("ken", 1L),
      ("ryu", 3L),
      ("chunli", 1L)
    )
  }

  it should "keep a table with all character selected" in {
    val inputs = Seq(("0000101", "terminal-8,Ryu"),
      ("0000102", "terminal-5,Jin"),
      ("0000103", "terminal-1,Ryu"),
      ("0000104", "terminal-3,Ken"),
      ("0000105", "terminal-7,Ryu"),
      ("0000106", "terminal-2,Chunli"),
      ("0000107", "terminal-2,Alen"))

    val topology = MockedStreams().topology(CompleteTopology.createTopology)

    val flow = topology.input("favourite-player-input", Serdes.String, Serdes.String, inputs)

    val result: Map[String, Long] = flow.outputTable("favourite-player-output-scala", Serdes.String, Serdes.Long, 6)

    result should have size 3

    result.keys should contain only ("ryu", "ken", "chunli")

    result("ryu") shouldBe 3
    result("ken") shouldBe 1
    result("chunli") shouldBe 1
  }
}
