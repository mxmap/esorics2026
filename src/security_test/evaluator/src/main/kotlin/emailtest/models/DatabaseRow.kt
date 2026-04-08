package emailtest.models

import kotlinx.serialization.Serializable
import java.io.Console

@Serializable
data class DatabaseRow(
    val domainName: String,
    val mxrecordName: String?,
    val ipAddress: String?,
    val hasDane: Boolean?,
    val daneConsoleLog: String?,
    val hasSslv2: Boolean?,
    val hasSslv3: Boolean?,
    val hasTls1: Boolean?,
    val hasTls1_1: Boolean?,
    val hasTls1_2: Boolean?,
    val hasTls1_3: Boolean?,
    val tlsConsoleLog: String?,
    val hasDmarc: Boolean?,
    val hasGoodDmarc: Boolean?,
    val hasSpf: Boolean?,
    val hasGoodSpf: Boolean?,
    val hasDkim: Boolean?,
    val dssConsoleLog: String?
)
