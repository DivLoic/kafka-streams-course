package com.github.simplesteph.udemy.scala.datagen

import com.sksamuel.avro4s.{AvroName, AvroNamespace}


sealed trait Game

object Game {
  case object StreetFighter extends Game
  case object Takken extends Game
  case object KingOfFighters extends Game
  case object SoulCalibur extends Game
  case object SamuraiShodown extends Game
}
