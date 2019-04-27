package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Instant;
import java.util.Optional;

import static com.github.simplesteph.udemy.kafka.streams.LowLevelProcessorApp.NO_REWARD_LEFT;

public class RewardTransformer implements Transformer<MachineId, Victory, KeyValue<MachineId, RewardedVictory>> {

    private ProcessorContext context;
    private TimestampedKeyValueStore<MachineId, Reward> rewardKeyValueStore;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.rewardKeyValueStore = (TimestampedKeyValueStore) this.context.getStateStore("reward-store-name");
    }

    @Override
    public KeyValue<MachineId, RewardedVictory> transform(MachineId key, Victory value) {

        Reward reward = this.rewardKeyValueStore.get(key).value();

        Optional<Coupon> optCoupon = reward
                .getCoupons()
                .stream()
                .filter(Coupon::getAvailability)
                .findFirst();

        RewardedVictory rewardedGame =  optCoupon
                .map((coupon) -> new RewardedVictory(
                                value.getGame(),
                                value.getTerminal(),
                                value.getTimestamp(),
                                value.getPlayer(),
                                coupon.getCode()
                        )

                )
                // their is no coupon left for this game
                .orElse(NO_REWARD_LEFT);

        // mark the coupon as used
        optCoupon.ifPresent(coupon -> {
            coupon.setAvailability(false);
            this.rewardKeyValueStore.put(key, ValueAndTimestamp.make(reward, Instant.now().toEpochMilli()));
        });

        return new KeyValue<>(key, rewardedGame);
    }

    @Override
    public void close() {

    }
}
