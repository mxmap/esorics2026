package models

import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable

@Serializable
data class ScanResult(
    val scanTime: Instant,
    val scanData: List<ScanData>,
    val testSSLResult: List<TestSSLResult>,
    val dssResult: List<DssResult>,
    val daneCheckResult: List<DaneCheckResult>
)