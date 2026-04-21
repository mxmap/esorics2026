package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class TlsResult(val host: String, val ip: String, val exitCode: Int, val consoleLog: String)

fun TlsResult.hasNoStartTLSSupport(): Boolean {
    val searchString = "=== we'll have to search for \"^250[ -]STARTTLS\" pattern ===\n"
    val searchStringIndex = consoleLog.indexOf(searchString)
    if (searchStringIndex == -1) return false
    val startOfResponseIndex = searchStringIndex + searchString.length
    val hasReceivedResponse = consoleLog.substring(startOfResponseIndex, startOfResponseIndex + 3) == "S: "
    val containsNoStartTLSErrorMessage =
        consoleLog.contains("=== finished smtp STARTTLS dialog with 3 ===\n\nFatal error: No STARTTLS found in handshake")

    return hasReceivedResponse && containsNoStartTLSErrorMessage
}

fun TlsResult.hasResultForProtocols() = consoleLog.contains("PROTOS_OFFERED:")

fun TlsResult.isValid() = hasResultForProtocols() || hasNoStartTLSSupport()

fun TlsResult.supportsTlsVersion(tlsVersion: TlsVersion): Boolean? {
    if (!isValid()) return null
    if (!hasResultForProtocols() && hasNoStartTLSSupport()) return false
    if (consoleLog.lines().find { it.contains("PROTOS_OFFERED:") }
            ?.contains(tlsVersion.version + ":yes") == true) return true
    return false
}
