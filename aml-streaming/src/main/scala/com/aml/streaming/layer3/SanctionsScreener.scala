package com.aml.streaming.layer3

import com.google.common.hash.{BloomFilter, Funnels}
import java.nio.charset.StandardCharsets

case class SanctionMatch(
  matchedName: String,
  listSource: String,
  similarity: Double
)

case class ScreeningResult(
  isHit: Boolean,
  matchResult: Option[SanctionMatch]
)

class SanctionsScreener(
  sanctionedNames: List[String],
  bloomFilter: BloomFilter[CharSequence],
  similarityThreshold: Double = 0.85
) {

  def screen(name: String): ScreeningResult = {
    // Phase 1: Bloom Filter fast check
    val normalized = normalize(name)
    if (!bloomFilter.mightContain(normalized)) {
      // If the exact normalized form isn't in the bloom filter,
      // still check with fuzzy matching against all sanctioned names
      // (bloom filter is an optimization for the common case)
      val fuzzyMatch = findBestMatch(normalized)
      return fuzzyMatch match {
        case Some((matched, score)) =>
          ScreeningResult(isHit = true, Some(SanctionMatch(matched, "OFAC", score)))
        case None =>
          ScreeningResult(isHit = false, None)
      }
    }

    // Phase 2: Precise fuzzy matching
    findBestMatch(normalized) match {
      case Some((matched, score)) =>
        ScreeningResult(isHit = true, Some(SanctionMatch(matched, "OFAC", score)))
      case None =>
        ScreeningResult(isHit = false, None)
    }
  }

  private def findBestMatch(normalized: String): Option[(String, Double)] = {
    sanctionedNames
      .map(sanctioned => (sanctioned, jaroWinkler(normalize(sanctioned), normalized)))
      .filter(_._2 >= similarityThreshold)
      .sortBy(-_._2)
      .headOption
  }

  private def normalize(name: String): String = {
    name.trim.toLowerCase
      .replaceAll("[^a-z0-9\\s]", "")
      .replaceAll("\\s+", " ")
  }

  // Jaro-Winkler similarity implementation
  def jaroWinkler(s1: String, s2: String): Double = {
    if (s1 == s2) return 1.0
    val len1 = s1.length
    val len2 = s2.length
    if (len1 == 0 || len2 == 0) return 0.0

    val matchDistance = Math.max(len1, len2) / 2 - 1
    val s1Matches = new Array[Boolean](len1)
    val s2Matches = new Array[Boolean](len2)

    var matches = 0
    var transpositions = 0

    for (i <- 0 until len1) {
      val start = Math.max(0, i - matchDistance)
      val end = Math.min(i + matchDistance + 1, len2)
      for (j <- start until end if !s2Matches(j) && s1(i) == s2(j)) {
        s1Matches(i) = true
        s2Matches(j) = true
        matches += 1
      }
    }

    if (matches == 0) return 0.0

    var k = 0
    for (i <- 0 until len1 if s1Matches(i)) {
      while (!s2Matches(k)) k += 1
      if (s1(i) != s2(k)) transpositions += 1
      k += 1
    }

    val jaro = (matches.toDouble / len1 + matches.toDouble / len2 +
      (matches - transpositions / 2.0) / matches) / 3.0

    // Winkler modification
    var prefix = 0
    for (i <- 0 until Math.min(4, Math.min(len1, len2)) if s1(i) == s2(i)) {
      prefix += 1
    }

    jaro + prefix * 0.1 * (1 - jaro)
  }
}

object SanctionsScreener {
  def create(sanctionedNames: List[String]): SanctionsScreener = {
    val bloomFilter = BloomFilter.create[CharSequence](
      Funnels.stringFunnel(StandardCharsets.UTF_8),
      sanctionedNames.size * 10,
      0.001
    )
    sanctionedNames.foreach(name => bloomFilter.put(name.toLowerCase.trim))
    new SanctionsScreener(sanctionedNames, bloomFilter)
  }
}
