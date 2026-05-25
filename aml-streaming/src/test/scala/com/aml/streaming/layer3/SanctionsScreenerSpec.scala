package com.aml.streaming.layer3

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SanctionsScreenerSpec extends AnyFlatSpec with Matchers {

  val screener = SanctionsScreener.create(List(
    "Osama bin Laden",
    "Kim Jong Un",
    "Vladimir Putin",
    "Nicolas Maduro"
  ))

  "SanctionsScreener" should "detect exact match" in {
    val result = screener.screen("Osama bin Laden")
    result.isHit shouldBe true
    result.matchResult.get.similarity shouldBe 1.0
  }

  it should "detect fuzzy match with minor typo" in {
    val result = screener.screen("Osama bin Ladin")
    result.isHit shouldBe true
    result.matchResult.get.similarity should be >= 0.85
  }

  it should "pass clean names" in {
    val result = screener.screen("John Smith")
    result.isHit shouldBe false
  }

  it should "detect partial name match" in {
    val result = screener.screen("Kim Jong-un")
    result.isHit shouldBe true
  }
}
