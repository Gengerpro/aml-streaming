package com.aml.batch.feature

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserProfileJobSpec extends AnyFlatSpec with Matchers {

  "UserProfileJob" should "calculate correct risk score" in {
    val factors = RiskFactors(
      countryRisk = 0.7,
      productRisk = 0.5,
      customerTypeRisk = 0.3,
      txnPatternRisk = 0.8
    )
    val score = UserProfileJob.calculateRiskScore(factors)
    score should be > 0.0
    score should be <= 1.0
  }

  it should "assign correct risk level" in {
    UserProfileJob.riskLevel(0.9) shouldBe "HIGH"
    UserProfileJob.riskLevel(0.6) shouldBe "MEDIUM"
    UserProfileJob.riskLevel(0.3) shouldBe "LOW"
  }
}
