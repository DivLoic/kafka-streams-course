package com.github.simplesteph.udemy.scala.datagen

import com.github.simplesteph.udemy.scala.datagen.Dataset.ExactlyOnceExercise.{Challenger, Hit}
import com.github.simplesteph.udemy.scala.datagen.Dataset.RYU
import io.circe.Decoder._
import io.circe.generic.auto._
import io.circe.parser._
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class ArcadeContestEOSSpec extends FlatSpec with Matchers with GivenWhenThen {

  "newRandomImpact" should "create a produce record containing a json" in  {

    Given("a instance of Challenger")
    val input = Challenger("john", RYU)

    When("newRandomImpact is called by the game")
    val result = ArcadeContestEOS.newRandomImpact(input)

    val key = result.key()
    val value = decode[Hit](result.value()).right.get

    Then("it produces a record with a json format value")
    key shouldBe "john"
    value.key should contain oneOf('X', 'O')
    value.challenger.login shouldBe "john"
    value.challenger.character.toLowerCase shouldBe "ryu"
    value.damage should (be >= 5 and be < 25)
  }

}
