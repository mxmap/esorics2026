import com.google.common.util.concurrent.RateLimiter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import models.CrawlerFile
import models.DomainMX
import models.ScanData
import org.slf4j.LoggerFactory
import java.io.File
import java.lang.Runtime.getRuntime

object Preprocessing {
    val logger = LoggerFactory.getLogger(Preprocessing::class.java)

    fun loadUniqueDomainsFromMailList(path: String) =
        loadMailAddresses(path)
            .map { it.splitDomain().lowercase() }
            .distinct()

    fun resolveMXRecords(domains: List<String>): List<DomainMX> {
        val counterMutex = Mutex()
        val listMutex = Mutex()
        var counter = 0
        val mxRecords = mutableListOf<DomainMX>()

        val threadCount = getRuntime().availableProcessors() * Config.get<Int>("DNS_THREAD_MULTIPLIER")
        val semaphore = Semaphore(threadCount)

        val rateLimiter = RateLimiter.create(Config["DNS_QUERIES_PER_SECOND"])

        logger.info("starting to resolve MX records from domains")
        logger.info("using $threadCount threads for resolving MX records")
        logger.info("using a rate limit of ${rateLimiter.rate.toInt()} queries per second")
        val starttime = System.currentTimeMillis()

        runBlocking {
            domains.forEach { domain ->
                launch(Dispatchers.IO) {
                    semaphore.withPermit {
                        rateLimiter.acquire()

                        counterMutex.withLock {
                            counter += 1
                            if (counter % (domains.size / 10).coerceAtLeast(1) == 0) {
                                logger.info("resolving MXRecord $counter / ${domains.size}. ${(100.0 * counter.toDouble() / domains.size).toInt()}%")
                            }
                        }

                        val digResult = Dig.dig(domain, "MX")
                        for (mx in digResult.answer) {
                            listMutex.withLock {
                                mxRecords.add(
                                    DomainMX(
                                        domain,
                                        mx,
                                        digResult.adFlagForMx
                                    )
                                )
                            }
                        }
                        if (digResult.answer.isEmpty()) listMutex.withLock { mxRecords.add(DomainMX(domain, null, null)) }
                    }
                }
            }
        }

        val records = mxRecords.distinct()
        logger.info(
            "resolved ${domains.size} domains to ${
                records.distinctBy
                { it.mxRecord }.count()
            } distinct MX records in ${"%.2f".format((System.currentTimeMillis() - starttime) / 1000.0)} seconds"
        )
        return records
    }

    fun resolveIPAddresses(domainMXList: List<DomainMX>): List<ScanData> {
        val mutex = Mutex()
        var counter = 0

        val scanData = mutableListOf<ScanData>()

        val threadCount = getRuntime().availableProcessors() * Config.get<Int>("DNS_THREAD_MULTIPLIER")
        val semaphore = Semaphore(threadCount)

        val rateLimiter = RateLimiter.create(Config["DNS_QUERIES_PER_SECOND"])

        logger.info("starting to resolve IP addresses from MX records")
        logger.info("using $threadCount threads for resolving IP addresses")
        logger.info("using a rate limit of ${rateLimiter.rate.toInt()} queries per second")

        val starttime = System.currentTimeMillis()

        runBlocking {
            val uniqueMXRecords = domainMXList.distinctBy { it.mxRecord }
            uniqueMXRecords.forEach { domainMX ->
                launch(Dispatchers.IO) {
                    semaphore.withPermit {


                        rateLimiter.acquire()
                        mutex.withLock {
                            counter++

                            if (counter % (uniqueMXRecords.size / 10).coerceAtLeast(1) == 0) {
                                logger.info(
                                    "resolving IPAddress $counter / ${uniqueMXRecords.size}. ${(100.0 * counter.toDouble() / uniqueMXRecords.size).toInt()}%"
                                )
                            }
                        }

                        val digResult = if (domainMX.mxRecord != null) Dig.dig(domainMX.mxRecord, "A") else null
                        if (digResult != null) {
                            for (a in digResult.answer) {
                                mutex.withLock {
                                    scanData.addAll(domainMXList.filter { it.mxRecord == domainMX.mxRecord }.map {
                                        ScanData(
                                            it.domain,
                                            it.mxRecord,
                                            a,
                                            it.adFlagForMx
                                        )
                                    })
                                }
                            }
                        }

                        if (digResult?.answer.isNullOrEmpty()) mutex.withLock {
                            scanData.addAll(domainMXList.filter { it.mxRecord == domainMX.mxRecord }.map {
                                ScanData(
                                    it.domain,
                                    it.mxRecord,
                                    null,
                                    null
                                )
                            })
                        }

                    }
                }
            }

        }

        val data = scanData.distinct()
        logger.info(
            "resolved ${domainMXList.size} MX Records to ${data.size} distinct domain + mx + ip combinations in ${
                "%.2f".format(
                    (System.currentTimeMillis() - starttime) / 1000.0
                )
            } seconds"
        )
        return data
    }

    private fun String.splitDomain(): String {
        return substring(indexOf("@") + 1)
    }

    private fun loadMailAddresses(path: String): List<String> {
        val jsonFormat = Json { ignoreUnknownKeys = true }
        val jsonRaw = readFileDirectlyAsText(path)
        return jsonFormat.decodeFromString(CrawlerFile.serializer(), jsonRaw).checked_emails
    }

    private fun readFileDirectlyAsText(fileName: String): String = File(fileName).readText(Charsets.UTF_8)
}
