package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class DssResult(val domain: String, val exitCode: Int, val consoleLog: String)

fun DssResult.isValid(): Boolean {
    return !consoleLog.contains("error:")
}

fun DssResult.hasDmarc(): Boolean {
    return consoleLog.lines().find { it.trim().startsWith("dmarc:") } != null
}

fun DssResult.hasGoodDmarc(): Boolean {
    if (!hasDmarc()) return false
    val entry = consoleLog.lines().find { it.trim().startsWith("dmarc:") } ?: ""
    return entry.contains("p=reject")
}

fun DssResult.hasDkim(): Boolean {
    return consoleLog.lines().find { it.trim().startsWith("dkim:") } != null
}

fun DssResult.hasSpf(): Boolean {
    return consoleLog.lines().find { it.trim().startsWith("spf:") } != null
}

fun DssResult.hasGoodSpf(): Boolean {
    if (!hasSpf()) return false
    val entry = consoleLog.lines().find { it.trim().startsWith("spf:") } ?: ""
    return entry.contains("-all")
}