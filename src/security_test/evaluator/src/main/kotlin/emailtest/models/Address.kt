package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class Address(val domain: String, val mxRecord: String?, val ipAddress: String?, val adFlagForMx: Boolean?)

fun Address.findTlsResult(tlsResults: List<TlsResult>) = tlsResults.find { it.host == mxRecord && it.ip == ipAddress }

fun Address.supportsTlsVersion(tlsResults: List<TlsResult>, tlsVersion: TlsVersion): Boolean? =
    findTlsResult(tlsResults)?.supportsTlsVersion(tlsVersion)

fun Address.isDaneOk(daneResults: List<DaneResult>): Boolean? {
    val daneResult = findDaneResult(daneResults) ?: return null
    if (adFlagForMx == true && daneResult.hasMtaDaneForMx(mxRecord!!, ipAddress!!)) return true
    return false
}

fun Address.findDaneResult(daneResults: List<DaneResult>) = daneResults.find { it.host == mxRecord }

fun Address.findDssResult(dssResults: List<DssResult>) = dssResults.find { it.domain == domain }

fun Address.hasDmarc(dssResults: List<DssResult>) = findDssResult(dssResults)?.hasDmarc()

fun Address.hasGoodDmarc(dssResults: List<DssResult>) = findDssResult(dssResults)?.hasGoodDmarc()

fun Address.hasSpf(dssResults: List<DssResult>) = findDssResult(dssResults)?.hasSpf()

fun Address.hasGoodSpf(dssResults: List<DssResult>) = findDssResult(dssResults)?.hasGoodSpf()

fun Address.hasDkim(dssResults: List<DssResult>) = findDssResult(dssResults)?.hasDkim()
