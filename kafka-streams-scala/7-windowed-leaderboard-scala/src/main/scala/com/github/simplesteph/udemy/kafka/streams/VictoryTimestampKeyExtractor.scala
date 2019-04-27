package com.github.simplesteph.udemy.kafka.streams

import com.github.simplesteph.Victory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

import scala.util.Try


class VictoryTimestampKeyExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long =
    Try(record.value.asInstanceOf[Victory].timestamp.toEpochMilli).getOrElse(previousTimestamp)

}
