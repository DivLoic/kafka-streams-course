package com.github.simplesteph.udemy.kafka.streams.internal

import scala.util.Random.shuffle

trait Generator {

  sealed abstract class Character(val name: String, val country: String)

  case object Ken extends Character("Ken", "US")
  case object Ryu extends Character("Ryu", "Japan")
  case object Geki extends Character("Geki", "Japan")
  case object Chunli extends Character("ChunLi", "China")
  case object Akuma extends Character("Akuma", "Japan")
  case object Sakura extends Character("Sakura", "Japan")
  case object Dhalsim extends Character("Dhalsim", "India")
  case object Blair extends Character("Blair", "UK")
  case object Blanka extends Character("BLANKA", "Brazil")

  val StreetFighterCast: Vector[Character] = Vector(Ryu, Ken, Chunli, Geki, Akuma, Sakura, Dhalsim, Blair, Blanka)

  protected def nextGame: String = shuffle(StreetFighterCast).take(2).map(_.name).reduce((a, b) => s"$a vs $b")

}
