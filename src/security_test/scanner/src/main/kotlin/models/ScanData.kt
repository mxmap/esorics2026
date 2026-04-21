package models

import kotlinx.serialization.Serializable

@Serializable
data class ScanData(val domain: String, val mxRecord: String?, val ipAddress: String?, val adFlagForMx: Boolean?)
