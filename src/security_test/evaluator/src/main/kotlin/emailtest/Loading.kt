package emailtest

import emailtest.models.*
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.json.Json
import java.io.File
import java.util.*

fun loadScanResult(filePath: String): ScanResult = loadJsonFromFile(ScanResult.serializer(), filePath)

private fun <T> loadJsonFromFile(deserializationStrategy: DeserializationStrategy<T>, filePath: String): T {
    val jsonRaw = File(filePath).readText(Charsets.UTF_8)
    return Json.decodeFromString(deserializationStrategy, jsonRaw)
}
