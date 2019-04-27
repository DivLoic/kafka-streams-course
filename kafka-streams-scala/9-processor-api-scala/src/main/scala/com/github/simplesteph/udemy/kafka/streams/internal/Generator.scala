package com.github.simplesteph.udemy.kafka.streams.internal

import java.time.Instant

import com.github.simplesteph.{Character, Coupon, Game, Player, Reward, Victory}
import org.scalacheck.Gen

import scala.util.Random


trait Generator {

  private val sfCharacters: Array[Character] = Array(
    Character("Ken", "US"),
    Character("Ryu", "Japan"),
    Character("Geki", "Japan"),
    Character("Chun-Li", "China"),
    Character("Akuma", "Japan"),
    Character("Sakura", "Japan"),
    Character("Dhalsim", "India"),
    Character("Blair", "UK")
  )

  private val takkenCharacters: Array[Character] = Array(
    Character("Jin", "Japan"),
    Character("Asuka", "Japan"),
    Character("Emilie", "Monaco"),
    Character("Kazuya", "Japan")
  )

  private val kofCharacters: Array[Character] = Array(
    Character("Mai", "Japan"),
    Character("Ramon", "Mexico"),
    Character("Nelson", "Brazil"),
    Character("Vanessa", "France")
  )

  private val scCharacters: Array[Character] = Array(
    Character("Kilik", "China"),
    Character("Ivy", "UK"),
    Character("Siegfried", "HRE"),
    Character("Nightmare", "X")
  )

  private val smCharacters: Array[Character] = Array(
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
    terminalId <- Gen.choose(0, 9)

    game <- Gen.oneOf(Game.values())

    character <- Gen.oneOf(CharactersMap(game))

    player <- Gen.frequency((4, Player.PlayerOne), (1, Player.PlayerTwo))

  } yield Victory(game, terminalId.toString, character, Instant.now(), player)

  protected def nextVictory: Option[Victory] = victoryGen.sample

  protected def nextRewards(bound: Int): Vector[Reward] = (0 to bound).map { termId =>
    Reward(
      termId.toString,
      coupons = (0 until 5) // <- 5 coupons for each machine
        .map { couponId =>
        Coupon(s"Coupon#$termId.0.$couponId", availability = true)
      }
    )
  }.toVector

  protected def nextValidStatus: Boolean = Random.nextBoolean()
}
