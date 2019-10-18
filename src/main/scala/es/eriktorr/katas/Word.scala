package es.eriktorr.katas

import java.sql.Timestamp

case class Word(word: String, timestamp: Timestamp) extends Watermark
