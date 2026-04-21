package models

import kotlinx.serialization.Serializable

@Serializable
data class CrawlerFile(val checked_emails: List<String>)
