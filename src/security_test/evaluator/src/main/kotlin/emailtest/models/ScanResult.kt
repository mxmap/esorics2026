package emailtest.models

import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable

@Serializable
data class ScanResult(
    val scanTime: Instant,
    val scanData: List<Address>,
    val testSSLResult: List<TlsResult>,
    val daneCheckResult: List<DaneResult>,
    val dssResult: List<DssResult>
)