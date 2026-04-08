package models

import kotlinx.serialization.Serializable

@Serializable
data class DssResult (
    val domain: String,
    val exitCode: Int,
    val consoleLog: String
)