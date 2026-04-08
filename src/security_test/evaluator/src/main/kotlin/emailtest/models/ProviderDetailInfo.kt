package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class ProviderDetailInfo(
    val provider: String,
    val daneAllMta: Boolean,
    val daneAtLeastOneMta: Boolean,
    val maxTlsAllMta: TlsVersion,
    val maxTlsAtLeastOneMta: TlsVersion,
    val minTlsAllMta: TlsVersion,
    val minTlsAtLeastOneMta: TlsVersion
)