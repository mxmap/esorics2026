package emailtest.analysis

import emailtest.models.*

fun createProviderInfo(validData: List<DatabaseRow>, daneResults: List<DaneResult>): ProviderInfo {
    val numDistinctProviders = validData.getProviderCount()

    val providerToDomainsMap = validData.providerToDomainsMap()
    val providerToDomainCountMap = validData.providerToManagedDomainsCountMap()

    val manageableDomainsCount = validData.providerToManagedDomainsCountMap().entries.sumOf { it.value }
    val countProvidersThatManageOver50Percent =
        validData.countOfProvidersThatManageMoreThanXPercentOfDomains(0.5)
    val biggestProviders =
        validData.providerToManagedDomainsCountMap().entries.take(countProvidersThatManageOver50Percent).map { it.key }

    val biggestProviderEntries = validData.filter { it.getProviderName()!! in biggestProviders }
    val biggestProviderDetailInfo = biggestProviderEntries.detailInfo(daneResults)


    val providerInfo = ProviderInfo(
        numDistinctProviders,
        manageableDomainsCount,
        providerToDomainCountMap,
        countProvidersThatManageOver50Percent,
        biggestProviderDetailInfo
    )

    return providerInfo
}

private fun List<DatabaseRow>.detailInfo(daneResults: List<DaneResult>): List<ProviderDetailInfo> {
    val allMtaMaxTlsVersion =
        tlsVersionGroupedBy(DatabaseRow::getProviderNameNotNull, AnalysisConfigTls.AllMtaMaxTlsVersion)
    val atLeastOneMtaMaxTlsVersion =
        tlsVersionGroupedBy(DatabaseRow::getProviderNameNotNull, AnalysisConfigTls.AtLeastOneMtaMaxTlsVersion)
    val allMtaMinTlsVersion =
        tlsVersionGroupedBy(DatabaseRow::getProviderNameNotNull, AnalysisConfigTls.AllMtaMinTlsVersion)
    val atLeastOneMtaMinTlsVersion =
        tlsVersionGroupedBy(DatabaseRow::getProviderNameNotNull, AnalysisConfigTls.AtLeastOneMtaMinTlsVersion)

    val daneAllMta = daneSupportForAllMtaBasedOnMxGroupedBy(daneResults, DatabaseRow::getProviderNameNotNull)
    val daneAtLeastOneMta = daneSupportForAtLeastOneMtaBasedOnMxGroupedBy(daneResults, DatabaseRow::getProviderNameNotNull)

    return allMtaMaxTlsVersion.mapKeys {
        ProviderDetailInfo(
            it.key,
            daneAllMta.getValue(it.key),
            daneAtLeastOneMta.getValue(it.key),
            it.value,
            atLeastOneMtaMaxTlsVersion.getValue(it.key),
            allMtaMinTlsVersion.getValue(it.key),
            atLeastOneMtaMinTlsVersion.getValue(it.key)
        )
    }.keys.toList()
}

fun List<DatabaseRow>.providerToDomainsMap() =
    groupBy { it.getProviderName()!! }.map {
        it.key to it.value.distinctBy { it.domainName }.map { it.domainName }
    }.toMap()

fun List<DatabaseRow>.providerToManagedDomainsCountMap() =
    providerToDomainsMap().map { it.key to it.value.count() }
        .sortedByDescending { it.second }.toMap()

fun List<DatabaseRow>.countOfProvidersThatManageMoreThanXPercentOfDomains(x: Double): Int {
    val domainCountTotal =
        providerToManagedDomainsCountMap().entries.sumOf { it.value }
    val domainCountTarget = domainCountTotal * x

    return providerToManagedDomainsCountMap().entries.scan(0) { acc, it -> acc + it.value }
        .count { it < domainCountTarget }
}

fun List<DatabaseRow>.getProviderCount() = getDistinctCount { it.getProviderName() }

fun DatabaseRow.getProviderName() = mxrecordName?.split(".")?.takeLast(2)?.joinToString(".")

fun DatabaseRow.getProviderNameNotNull() = mxrecordName?.split(".")?.takeLast(2)?.joinToString(".")!!