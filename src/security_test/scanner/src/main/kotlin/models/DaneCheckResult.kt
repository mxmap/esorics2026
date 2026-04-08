package models

import kotlinx.serialization.Serializable

@Serializable
data class DaneCheckResult(
    val host: String,
    val exitCode: Int,
    val consoleLog: String
)
