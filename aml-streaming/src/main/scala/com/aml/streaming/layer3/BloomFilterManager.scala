package com.aml.streaming.layer3

import com.google.common.hash.{BloomFilter, Funnels}
import java.nio.charset.StandardCharsets

class BloomFilterManager(expectedInsertions: Int = 100000, fpp: Double = 0.001) {

  @volatile private var currentFilter: BloomFilter[CharSequence] = createEmpty()

  private def createEmpty(): BloomFilter[CharSequence] = {
    BloomFilter.create[CharSequence](
      Funnels.stringFunnel(StandardCharsets.UTF_8),
      expectedInsertions,
      fpp
    )
  }

  def add(name: String): Unit = {
    currentFilter.put(normalize(name))
  }

  def mightContain(name: String): Boolean = {
    currentFilter.mightContain(normalize(name))
  }

  def rebuild(names: List[String]): Unit = {
    val newFilter = createEmpty()
    names.foreach(name => newFilter.put(normalize(name)))
    currentFilter = newFilter
  }

  def merge(names: List[String]): Unit = {
    names.foreach(name => currentFilter.put(normalize(name)))
  }

  private def normalize(name: String): String = {
    name.trim.toLowerCase
      .replaceAll("[^a-z0-9\\s]", "")
      .replaceAll("\\s+", " ")
  }
}
