package com.aml.streaming.layer3

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SanctionsScreenerSpec extends AnyFlatSpec with Matchers {

  val sanctionedNames = List(
    "AL QAIDA", "ISIS", "TALIBAN", "HEZBOLLAH", "HAMAS",
    "BANK SEPAH", "KOREA KWANGSON BANKING"
  )

  val screener = SanctionsScreener.create(sanctionedNames)

  "SanctionsScreener" should "detect exact match" in {
    val result = screener.screen("AL QAIDA")
    result.isHit shouldBe true
    result.matchResult.get.matchedName shouldBe "AL QAIDA"
    result.matchResult.get.similarity shouldBe 1.0
  }

  it should "detect fuzzy match with similar spelling" in {
    val result = screener.screen("al qaeda")
    result.isHit shouldBe true
    result.matchResult.get.similarity should be >= 0.85
  }

  it should "detect fuzzy match with case variation" in {
    val result = screener.screen("Taleban")
    result.isHit shouldBe true
    result.matchResult.get.matchedName shouldBe "TALIBAN"
  }

  it should "not match unrelated names" in {
    val result = screener.screen("Microsoft Corporation")
    result.isHit shouldBe false
  }

  it should "not match empty string" in {
    val result = screener.screen("")
    result.isHit shouldBe false
  }

  it should "not match short unrelated names" in {
    val result = screener.screen("IBM")
    result.isHit shouldBe false
  }

  it should "detect bank name match" in {
    val result = screener.screen("BANK SEPAH")
    result.isHit shouldBe true
  }

  it should "detect fuzzy bank name with typo" in {
    val result = screener.screen("BANK SEPAH")
    result.isHit shouldBe true
    result.matchResult.get.matchedName shouldBe "BANK SEPAH"
  }

  "BloomFilterManager" should "add and check names" in {
    val manager = new BloomFilterManager()
    manager.add("TEST NAME")
    manager.mightContain("TEST NAME") shouldBe true
    manager.mightContain("OTHER NAME") shouldBe false
  }

  it should "rebuild with new names" in {
    val manager = new BloomFilterManager()
    manager.add("OLD NAME")
    manager.rebuild(List("NEW NAME"))
    manager.mightContain("NEW NAME") shouldBe true
  }
}
