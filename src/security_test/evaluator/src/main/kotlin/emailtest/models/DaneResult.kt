package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class DaneResult(val host: String, val exitCode: Int, val consoleLog: String)

fun DaneResult.hasMtaDaneForMx(mxRecord: String, ipAddress: String): Boolean {
    val mtaResultIndex = consoleLog.split("\n").indexOf("## Checking $mxRecord $ipAddress port 25")
    if (consoleLog.split("\n")
            .filterIndexed { index, _ -> index >= mtaResultIndex }
            .takeWhile { it.isNotBlank() }
            .find { it.startsWith("Result: DANE OK") } != null
    ) return true
    return false
}