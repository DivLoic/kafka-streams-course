package com.github.simplesteph.udemy.kafka.streams

import java.time.Instant

import com.github.simplesteph.udemy.kafka.streams.LowLevelProcessorApp.RewardedVictoryError
import com.github.simplesteph._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{TimestampedKeyValueStore, ValueAndTimestamp}

class RewardTransformer extends Transformer[MachineId, Victory, KeyValue[MachineId, RewardedVictory]] {

  private var context: ProcessorContext = _
  private var rewardKeyValueStore: TimestampedKeyValueStore[MachineId, Reward] = _

  override def init(context: ProcessorContext): Unit = {
    this.context = context
    this.rewardKeyValueStore = this.context
      .getStateStore("reward-store-name-scala")
      .asInstanceOf[TimestampedKeyValueStore[MachineId, Reward]]
  }

  override def transform(key: MachineId, value: Victory): KeyValue[MachineId, RewardedVictory] = {

    val reward: Reward = this.rewardKeyValueStore.get(key).value()

    val optCoupon: Option[Coupon] = reward
      .coupons
      .find(_.availability)

    val rewardedGame: RewardedVictory = optCoupon.map { coupon =>
      new RewardedVictory(
        value.game,
        value.terminal,
        value.datetime,
        value.player,
        coupon.code
      )
      // their is no coupon left for this game
    }.getOrElse(RewardedVictoryError)

    // mark the coupon as used
    optCoupon.foreach(coupon => {
      val usedCouponIdx = reward.coupons.indexOf(coupon)
      val updatedCoupon = coupon.copy(availability = false)
      val updatedReward = reward.copy(coupons = reward.coupons.updated(usedCouponIdx, updatedCoupon))

      this.rewardKeyValueStore.put(key, ValueAndTimestamp.make(updatedReward, Instant.now().toEpochMilli))
    })

    new KeyValue(key, rewardedGame)
  }

  override def close(): Unit = {}
}
