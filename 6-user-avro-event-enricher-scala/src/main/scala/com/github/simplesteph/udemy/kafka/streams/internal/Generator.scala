package com.github.simplesteph.udemy.kafka.streams.internal

import com.github.simplesteph.{Game, User}
import org.scalacheck.Gen
import org.scalacheck.Gen.Parameters
import org.scalacheck.rng.Seed


trait Generator {

  import io.alphash.faker._

  val seed: Seed = Seed(60L)

  val param: Parameters = Parameters.default

  val generator = Gen.pureApply(param, seed)

  def nexUser(users: Vector[User]): Option[User] = generator
      .frequency((3, true), (4, false))
      .flatMap { isKnownUser =>
        if (isKnownUser) Gen.oneOf(users)
        else {
          val person = Person()
          Gen.const(
            User(
              s"${person.name.head}${person.lastName}${Gen.chooseNum(0, 9).sample.getOrElse("")}",
              person.name,
              Some(person.lastName))
          )
        }
      }.sample

  def nextIsTwoPlayer: Option[Boolean] = generator.frequency((2, true), (5, false)).sample

  def nextPurchaseId: Option[String] =  generator.uuid.map(_.toString.take(8)).sample

  def nextGame: Option[Game] = generator.oneOf(Game.values).sample

}
