package com.github.simplesteph.udemy.scala.datagen

import java.time.Instant

object Dataset {

  sealed abstract class Character(val name: String, val country: String)

  case object KEN extends Character("Ken", "US")
  case object RYU extends Character("Ryu", "Japan")
  case object GEKI extends Character("Geki", "Japan")
  case object CHUNLI extends Character("ChunLi", "China")
  case object AKUMA extends Character("Akuma", "Japan")
  case object SAKURA extends Character("Sakura", "Japan")
  case object DHALSIM extends Character("Dhalsim", "India")
  case object BLAIR extends Character("Blair", "UK")
  case object BLANKA extends Character("BLANKA", "Brazil")

  val StreetFighterCast: Vector[Character] = Vector(RYU, KEN, CHUNLI, GEKI, AKUMA, SAKURA, DHALSIM, BLAIR)

  sealed trait Game

  case object StreetFighter extends Game
  case object Takken extends Game
  case object KingOfFighters extends Game
  case object SoulCalibur extends Game
  case object SamuraiShodown extends Game

  val GameCollection: Vector[Game] = Vector(StreetFighter, Takken, KingOfFighters, SoulCalibur, SamuraiShodown)

  case class User(login: String, firstName: String, lastName: Option[String] = None, email: Option[String] = None)



  /* helpers */

  object User {

    def apply(login: String, firstName: String, lastName: String, email: String): User =
      new User(login, firstName, Some(lastName), Some(email))

    def apply(login: String, firstName: String, lastName: String): User =
      new User(login, firstName, Some(lastName))
  }

  case class Purchase(user: String, game: Game, twoPlayer: Boolean)

  object ExactlyOnceExercise {

    case class Challenger(login: String, character: String)

    object Challenger {
      def apply(login: String, character: Character): Challenger =
        new Challenger(login, character.name)
    }
    case class Hit(key: String, challenger: Challenger, damage: Int, time: Instant)

  }
}
