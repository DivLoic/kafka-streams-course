package com.github.simplesteph.udemy.kafka.streams

import java.time.Instant

package object internal {

  abstract class Character(val name: String, val country: String)

  case class Challenger(login: String, character: String)

  case class Hit(key: String, challenger: Challenger, damage: Int, time: Instant)

  object Challenger {
    def apply(login: String, character: Character): Challenger =
      new Challenger(login, character.name)
  }
}
