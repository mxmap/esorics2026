package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class DomainInfo(
    val total: Int,
    val totalWithoutMXRecord: Int,
    val numDomainsNoResolvableMxRecord: Int,
    val numDomainsWithOnlyValidResults: Int,
    val numDomainsWithBothValidAndInvalidResults: Int,
    val numDomainsWithNoValidResults: Int,
    val cleanedDataDomains: Int,
    //val maxTlsVersionAllMtaDomains: Map<TlsVersion, List<String>>,
    val maxTlsVersionAllMtaDomainCount: Map<TlsVersion, Int>,
    //val maxTlsVersionAtLeastOneMtaDomains: Map<TlsVersion, List<String>>,
    val maxTlsVersionAtLeastOneMtaDomainCount: Map<TlsVersion, Int>,
    //val minTlsVersionAllMtaDomains: Map<TlsVersion, List<String>>,
    val minTlsVersionAllMtaDomainCount: Map<TlsVersion, Int>,
    //val minTlsVersionAtLeastOneMtaDomains: Map<TlsVersion, List<String>>,
    val minTlsVersionAtLeastOneMtaDomainCount: Map<TlsVersion, Int>,
    //val domainsMixedMaxTls: List<String>,
    val domainsMixedMaxTlsCount: Int,
    //val domainsMixedMinTls: List<String>,
    val domainsMixedMinTlsCount: Int,
    //val daneAllMtaDomains: List<String>,
    val daneAllMtaDomainCount: Int,
    //val daneAtLeastOneMtaDomains: List<String>,
    val daneAtLeastOneMtaDomainCount: Int,
    //val secureDomains: List<String>,
    val secureDomainCount: Int,
    //val domainsDaneForAllMtaBasedOnMx: Map<Boolean, List<String>>,
    val domainsDaneForAllMtaBasedOnMxCount: Map<Boolean, Int>,
    //val domainsDaneForAtLeastOneMtaBasedOnMx: Map<Boolean, List<String>>,
    val domainsDaneForAtLeastOneMtaBasedOnMxCount: Map<Boolean, Int>,
    val domainsWithDmarc: Int,
    val domainsWithGoodDmarc: Int,
    val domainsWithSpf: Int,
    val domainsWithGoodSpf: Int,
    val domainsWithDkim: Int
    )