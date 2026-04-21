package emailtest

import Config
import emailtest.analysis.*
import emailtest.models.*
import kotlinx.serialization.json.Json
import java.io.File
import java.util.EnumMap

fun main(args: Array<String>) {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, Config["LOG_LEVEL"])

    val inputDirectory = File("./evaluator-input")
    inputDirectory.listFiles { file -> file.isFile && file.extension.lowercase() == "json" }?.forEach { file ->
        // load raw data
        val scanResult = loadScanResult(file.absolutePath)
        val fileName = file.nameWithoutExtension
        RunEvaluation.logger.info("loaded scan result from $fileName")

        val addresses = scanResult.scanData
        val daneResults = scanResult.daneCheckResult
        val tlsResults = scanResult.testSSLResult
        val dssResults = scanResult.dssResult

        // transform to database row data
        val data = addresses.map {
            DatabaseRow(
                it.domain,
                it.mxRecord,
                it.ipAddress,
                it.isDaneOk(daneResults),
                it.findDaneResult(daneResults)?.consoleLog,
                it.supportsTlsVersion(tlsResults, TlsVersion.SSLV2),
                it.supportsTlsVersion(tlsResults, TlsVersion.SSLV3),
                it.supportsTlsVersion(tlsResults, TlsVersion.TLS1),
                it.supportsTlsVersion(tlsResults, TlsVersion.TLS1_1),
                it.supportsTlsVersion(tlsResults, TlsVersion.TLS1_2),
                it.supportsTlsVersion(tlsResults, TlsVersion.TLS1_3),
                it.findTlsResult(tlsResults)?.consoleLog,
                it.hasDmarc(dssResults),
                it.hasGoodDmarc(dssResults),
                it.hasSpf(dssResults),
                it.hasGoodSpf(dssResults),
                it.hasDkim(dssResults),
                it.findDssResult(dssResults)?.consoleLog
            )
        }

        // filter valid test results
        val validData =
            data.filter { it.isTLSValid() && it.isDANEValid() && it.mxrecordName != null && it.ipAddress != null }

        // analyze
        val domainInfo = createDomainInfo(data, validData, daneResults)
        val providerInfo = createProviderInfo(validData, daneResults)
        val mxRecordInfo = createMxRecordInfo(data)

        val evaluation = RunEvaluation(
            scanResult.scanTime,
            domainInfo,
            providerInfo,
            mxRecordInfo
        )

        // save results
        val jsonFormatter = Json { prettyPrint = true }
        val resultJson = jsonFormatter.encodeToString(RunEvaluation.serializer(), evaluation)
        writeJSONFile(resultJson, "./evaluator-result/" + fileName.substringBefore('_') +"_evaluation.json")
        RunEvaluation.logger.info("finished scan result from $fileName")
    }
    RunEvaluation.logger.info("evaluation finished")
}


