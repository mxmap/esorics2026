package emailtest.analysis

import emailtest.models.DaneResult
import emailtest.models.DatabaseRow
import emailtest.models.TlsVersion
import emailtest.models.hasMtaDaneForMx
import java.io.File

fun DatabaseRow.isTLSValid() = this.hasSslv2 != null &&
        this.hasSslv3 != null &&
        this.hasTls1 != null &&
        this.hasTls1_1 != null &&
        this.hasTls1_2 != null &&
        this.hasTls1_3 != null

fun DatabaseRow.isDANEValid() = this.hasDane != null

fun DatabaseRow.availableTlsVersions(): List<TlsVersion> {
    if (!isTLSValid()) throw IllegalArgumentException("only valid TLS results allowed")

    val availableVersions = arrayListOf<TlsVersion>()

    if (hasTls1_3 == true) availableVersions.add(TlsVersion.TLS1_3)
    if (hasTls1_2 == true) availableVersions.add(TlsVersion.TLS1_2)
    if (hasTls1_1 == true) availableVersions.add(TlsVersion.TLS1_1)
    if (hasTls1 == true) availableVersions.add(TlsVersion.TLS1)
    if (hasSslv3 == true) availableVersions.add(TlsVersion.SSLV3)
    if (hasSslv2 == true) availableVersions.add(TlsVersion.SSLV2)
    if (hasSslv2 == false && hasSslv3 == false && hasTls1 == false && hasTls1_1 == false && hasTls1_2 == false && hasTls1_3 == false) availableVersions.add(
        TlsVersion.NO_STARTTLS
    )

    return availableVersions
}

fun DatabaseRow.highestTlsVersion(): TlsVersion {
    val availableVersion = availableTlsVersions()
    return availableVersion.highestTlsVersion()
}

fun DatabaseRow.lowestTlsVersion(): TlsVersion {
    val availableVersion = availableTlsVersions()
    return availableVersion.lowestTlsVersion()
}

fun List<TlsVersion>.lowestTlsVersion(): TlsVersion {
    return minByOrNull { it.ordinal }!!
}

fun List<TlsVersion>.highestTlsVersion(): TlsVersion {
    return maxByOrNull { it.ordinal }!!
}

fun writeJSONFile(content: String, fileName: String) {
    val outFile = File(fileName)
    outFile.writeText(content)
}

enum class AnalysisConfigTls {
    AllMtaMinTlsVersion,
    AtLeastOneMtaMinTlsVersion,
    AllMtaMaxTlsVersion,
    AtLeastOneMtaMaxTlsVersion,
}

fun List<DatabaseRow>.tlsVersionGroupedBy(
    groupBy: (DatabaseRow) -> String,
    analysisConfig: AnalysisConfigTls,
): Map<String, TlsVersion> {
    val firstFunc = when (analysisConfig) {
        AnalysisConfigTls.AllMtaMinTlsVersion, AnalysisConfigTls.AtLeastOneMtaMinTlsVersion -> DatabaseRow::lowestTlsVersion
        AnalysisConfigTls.AllMtaMaxTlsVersion, AnalysisConfigTls.AtLeastOneMtaMaxTlsVersion -> DatabaseRow::highestTlsVersion
    }

    val secondFunc = when (analysisConfig) {
        AnalysisConfigTls.AllMtaMinTlsVersion -> List<TlsVersion>::highestTlsVersion
        AnalysisConfigTls.AtLeastOneMtaMinTlsVersion -> List<TlsVersion>::lowestTlsVersion
        AnalysisConfigTls.AllMtaMaxTlsVersion -> List<TlsVersion>::lowestTlsVersion
        AnalysisConfigTls.AtLeastOneMtaMaxTlsVersion -> List<TlsVersion>::highestTlsVersion
    }

    val groupToListOfMtaTlsVersion = groupBy { groupBy(it) }.mapValues {
        it.value.map { firstFunc(it) }
    }

    return groupToListOfMtaTlsVersion.mapValues { secondFunc(it.value) }
}

fun List<DatabaseRow>.daneSupportForAllMtaGroupedBy(groupBy: (DatabaseRow) -> String) =
    groupBy { groupBy(it) }.filter { it.value.all { row -> row.hasDane == true } }.map { it.key }

fun List<DatabaseRow>.daneSupportForAtLeastOneMtaGroupedBy(groupBy: (DatabaseRow) -> String) =
    groupBy { groupBy(it) }.filter { it.value.any { row -> row.hasDane == true } }.map { it.key }

fun List<DatabaseRow>.daneSupportForAllMtaBasedOnMxGroupedBy(daneResults: List<DaneResult>, groupBy: (DatabaseRow) -> String): Map<String, Boolean> {
    return groupBy { groupBy(it) }
        .mapValues {
            it.value.isNotEmpty().and(it.value.all { dataRow ->
                daneResults.find { it.host == dataRow.mxrecordName }!!
                    .hasMtaDaneForMx(dataRow.mxrecordName!!, dataRow.ipAddress!!)
            })
        }
}

fun List<DatabaseRow>.daneSupportForAtLeastOneMtaBasedOnMxGroupedBy(daneResults: List<DaneResult>, groupBy: (DatabaseRow) -> String): Map<String, Boolean> {
    return groupBy { groupBy(it) }
        .mapValues {
            it.value.isNotEmpty().and(it.value.any { dataRow ->
                daneResults.find { it.host == dataRow.mxrecordName }!!
                    .hasMtaDaneForMx(dataRow.mxrecordName!!, dataRow.ipAddress!!)
            })
        }
}

fun <K,V >Map<K,V>.reverse() = entries.map { (first, second) -> second to first }
    .groupBy { it.first }.mapValues { it.value.map { it.second } }

fun <K, V> Map<K, List<V>>.countValueList() = mapValues { it.value.count() }

fun <K, V> Map<K, V>.countKeys() = keys.count()

fun Boolean.toInt() = if (this) 1 else 0


