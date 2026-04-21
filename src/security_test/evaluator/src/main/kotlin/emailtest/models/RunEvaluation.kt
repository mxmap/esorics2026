package emailtest.models

import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory

@Serializable
data class RunEvaluation(
    val scanTime: Instant,
    val domainInfo: DomainInfo,
    val providerInfo: ProviderInfo,
    val mxRecordInfo: MXRecordInfo
) {
    companion object {
        val logger = LoggerFactory.getLogger(RunEvaluation::class.java)
    }
}



