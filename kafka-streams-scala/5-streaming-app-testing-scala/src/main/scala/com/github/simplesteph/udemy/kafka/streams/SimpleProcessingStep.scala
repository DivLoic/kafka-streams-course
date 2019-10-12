package com.github.simplesteph.udemy.kafka.streams


object SimpleProcessingStep {

  def formatKeyValue: (String, String) => (Int, String) =
    (key: String, value: String) => (key.toInt, value.split(",")(1).toLowerCase)

  def getMachineId: String => String = (value: String) =>
    "^terminal-(\\d+),.*$".r.findFirstMatchIn(value).map(_.group(1)).orNull

  def isTopCharacter: (String, String) => Boolean = (_: String, value: String) =>
    "ryu" :: "ken" :: "chunli" :: Nil contains value
}
