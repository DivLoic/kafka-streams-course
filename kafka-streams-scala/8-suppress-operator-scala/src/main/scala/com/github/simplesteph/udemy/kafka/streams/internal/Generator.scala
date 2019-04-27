package com.github.simplesteph.udemy.kafka.streams.internal

import java.time.Instant

import com.github.simplesteph.{Character, Game, Victory}
import org.scalacheck.{Arbitrary, Gen}

trait Generator {

  private val DamagedTerminal = "7"

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
      Game.Takken -> takkenCharacters,
      Game.None -> Array(Character("X", ""))
  )

  val victoryGen: Gen[Victory] = for {

    game <- Gen.oneOf(Game.values()).flatMap(g => Gen.frequency((6, Game.None), (4, g)))

    lateStatus <- Arbitrary.arbBool.arbitrary.map(_ && game == Game.None)

    character <- Gen.oneOf(CharactersMap(game))

    eventTime <- Gen.choose(5, 10).map { delay =>
      if(lateStatus) Instant.now().minusSeconds(delay)
      else Instant.now()
    }

  } yield Victory(game, DamagedTerminal, character, eventTime, Some(lateStatus))

  protected def nextVictory: Option[Victory] = victoryGen.sample
}
