package emailtest.models

import kotlinx.serialization.Serializable

@Serializable
data class MXRecordInfo(
    val total: Int
)