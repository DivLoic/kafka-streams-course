package com.github.simplesteph.udemy.kafka.streams

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

  val StreetFighterCast: Vector[Character] = Vector(RYU, KEN, CHUNLI, GEKI, AKUMA, SAKURA, DHALSIM, BLAIR, BLANKA)

}
