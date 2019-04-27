package com.github.simplesteph.udemy.kafka.streams.internal

import java.time.Instant

import com.github.simplesteph.{Character, Game, Player, Victory}
import org.scalacheck.Gen

trait Generator {

  private val sfCharacters = Array(
    Character("Ken", "US"),
    Character("Ryu", "Japan"),
    Character("Geki", "Japan"),
    Character("Chun-Li", "China"),
    Character("Akuma", "Japan"),
    Character("Sakura", "Japan"),
    Character("Dhalsim", "India"),
    Character("Blair", "UK")
  )

  private val takkenCharacters = Array(
    Character("Jin", "Japan"),
    Character("Asuka", "Japan"),
    Character("Emilie", "Monaco"),
    Character("Kazuya", "Japan")
  )

  private val kofCharacters = Array(
    Character("Mai", "Japan"),
    Character("Ramon", "Mexico"),
    Character("Nelson", "Brazil"),
    Character("Vanessa", "France")
  )

  private val scCharacters = Array(
    Character("Kilik", "China"),
    Character("Ivy", "UK"),
    Character("Siegfried", "HRE"),
    Character("Nightmare", "X")
  )

  private val smCharacters = Array(
    Character("Galford", "US"),
    Character("Charlotte", "France"),
    Character("Haohmaru", "Japan"),
    Character("Ukyo Tachibana", "Japan")
  )

  private val CharactersMap = Map(
    Game.KingOfFighters -> kofCharacters,
    Game.SamuraiShodown -> smCharacters,
    Game.StreetFighter -> sfCharacters,
    Game.SoulCalibur -> scCharacters,
    Game.Takken -> takkenCharacters
  )

  val victoryGen: Gen[Victory] = for {

    id <- Gen.chooseNum(0, 2)

    game <- Gen.oneOf(Game.values())

    character <- Gen.oneOf(CharactersMap(game))

    player <- Gen.oneOf(Player.values())

  } yield Victory(game, id.toString, character, Instant.now(), player)

  protected def nextVictory: Option[Victory] = victoryGen.sample

  protected def nextDelay: Option[Int] = Gen.chooseNum(1, 3).sample

}
