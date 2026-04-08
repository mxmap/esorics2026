package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class ProviderInfo(
    val total: Int,
    val manageableDomainsCount: Int,
    val domainCountPerProvider: Map<String, Int>,
    val providersFiftyPercentDomains: Int,
    val biggestProviderDaneTls: List<ProviderDetailInfo>
)