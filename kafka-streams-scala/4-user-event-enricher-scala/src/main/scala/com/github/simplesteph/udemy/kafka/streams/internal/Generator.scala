package com.github.simplesteph.udemy.kafka.streams.internal

import com.github.simplesteph.udemy.kafka.streams.GameGenerator.{GameCollection, clients}
import org.scalacheck.Gen

trait Generator {

  sealed trait Game

  case object StreetFighter extends Game
  case object Takken extends Game
  case object KingOfFighters extends Game
  case object SoulCalibur extends Game
  case object SamuraiShodown extends Game

  val GameCollection: Vector[Game] = Vector(StreetFighter, Takken, KingOfFighters, SoulCalibur, SamuraiShodown)

  case class UserKey(login: String)

  case class PurchaseKey(id: String, clientId: UserKey)

  case class Purchase(user: String, game: Game, istwoPlayer: Boolean)

  case class User(login: String, firstName: String, lastName: Option[String] = None, email: Option[String] = None)

  object User {

    def apply(login: String, firstName: String, lastName: String, email: String): User =
      new User(login, firstName, Some(lastName), Some(email))

    def apply(login: String, firstName: String, lastName: String): User =
      new User(login, firstName, Some(lastName))
  }

  def nextPurchase(clientsProvider: => Vector[User]): Option[Purchase] = {
    val genPurchase = for {

      knownClient <- Gen.oneOf(clientsProvider)
      unknownClient <- Gen.chooseNum(0, 999).map(i => User(s"Unknown$i", ""))

      client <-  Gen.frequency((3, knownClient), (4, unknownClient))

      game <- Gen.oneOf(GameCollection)

      twoPlayerMode <- Gen.frequency((1, true), (1, false))

    } yield Purchase(client.login, game, twoPlayerMode)

    genPurchase.sample
  }
}
