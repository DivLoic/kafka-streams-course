package com.github.simplesteph.udemy.scala.datagen

sealed trait Game

object Game {
  case object StreetFighter extends Game
  case object Takken extends Game
  case object KingOfFighters extends Game
  case object SoulCalibur extends Game
  case object SamuraiShodown extends Game

  val GameCollection: Vector[Game] = Vector(StreetFighter, Takken, KingOfFighters, SoulCalibur, SamuraiShodown)
}
