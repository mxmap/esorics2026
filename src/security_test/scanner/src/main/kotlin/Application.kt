import com.google.common.util.concurrent.RateLimiter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.datetime.Clock
import kotlinx.serialization.json.Json
import models.*
import org.slf4j.LoggerFactory
import processes.*
import java.io.File

class Application

fun main() {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, Config["LOG_LEVEL"]);

    val logger = LoggerFactory.getLogger(Application::class.java)

    val startTime = System.currentTimeMillis()
    logger.info("starting email test")

    val rateLimiterDane = RateLimiter.create(Config["DANE_TRIES_PER_SECOND"])
    val rateLimiterTls = RateLimiter.create(Config["TLS_TRIES_PER_SECOND"])
    val rateLimiterDss = RateLimiter.create(Config["DSS_TRIES_PER_SECOND"])

    val inputDirectory = File("./scanner-input")
    inputDirectory.listFiles { file -> file.isFile && file.extension.lowercase() == "json" }?.forEach { file ->

        val domains = Preprocessing.loadUniqueDomainsFromMailList(file.absolutePath)
        val fileName = file.nameWithoutExtension
        logger.info("loaded ${domains.size} unique domains from " + fileName)

        val mxRecords = Preprocessing.resolveMXRecords(domains)
        val uniqueMXRecords = mxRecords.distinctBy { it.mxRecord }
        logger.info("loaded ${uniqueMXRecords.size} unique mx records")

        val scanData = Preprocessing.resolveIPAddresses(mxRecords)
        logger.info("loaded ${scanData.size} unique combinations of domain + mx + ip")
        val uniqueMXAndIP =
            scanData.distinctBy { it.mxRecord + it.ipAddress }.filter { it.mxRecord != null && it.ipAddress != null }
                .map { ScanDataNotNullable(it.domain, it.mxRecord!!, it.ipAddress!!) }
        logger.info("loaded ${uniqueMXAndIP.size} unique combinations of mx + ip")

        val mxRecordsNotNull = mxRecords.filter { it.mxRecord != null }.map { it.mxRecord!! }.distinct()

        val scanTime = Clock.System.now()

        val testSSLResults = mutableListOf<TestSSLResult>()
        val dssResults = mutableListOf<DssResult>()
        val daneMxResults = mutableListOf<DaneCheckResult>()

        val mutex = Mutex()

        val threadCount = Runtime.getRuntime().availableProcessors() * Config.get<Int>("SCAN_THREAD_MULTIPLIER")
        val semaphore = Semaphore(threadCount)

        logger.info("using $threadCount threads for scan")
        logger.info("using a scan rate limit of ${rateLimiterDane.rate.toInt()} requests per second for DANE")
        logger.info("using a scan rate limit of ${rateLimiterTls.rate.toInt()} requests per second for TLS")

        runBlocking {
            var finishCounter = 0
            var startCounter = 0

            logger.info("starting tls scan")

            uniqueMXAndIP.forEach { mxAndIP ->
                launch(Dispatchers.IO) {
                    semaphore.withPermit {
                        mutex.withLock {
                            startCounter += 1
                            logger.info("tls scan: started $startCounter from ${uniqueMXAndIP.size}")
                        }

                        val tlsResult =
                            testTls(mxAndIP.mxRecord, mxAndIP.ipAddress, rateLimiterTls, Config["TLS_RETRIES"])

                        mutex.withLock {
                            testSSLResults.add(
                                TestSSLResult(
                                    mxAndIP.mxRecord,
                                    mxAndIP.ipAddress,
                                    tlsResult.exitCode,
                                    tlsResult.consoleLog
                                )
                            )

                            finishCounter += 1
                            logger.info("tls scan: finished $finishCounter from ${uniqueMXAndIP.size}")
                        }
                    }
                }
            }
        }
        logger.info("finished tls scan")

        runBlocking {
            var finishCounter = 0
            var startCounter = 0

            logger.info("starting dss scan")

            domains.forEach { domain ->
                launch(Dispatchers.IO) {
                    semaphore.withPermit {
                        mutex.withLock {
                            startCounter += 1
                            logger.info("dss scan: started $startCounter from ${domains.size}")
                        }

                        val dssResult = testDss(domain, rateLimiterDss, Config["TLS_RETRIES"])

                        mutex.withLock {
                            dssResults.add(
                                DssResult(
                                    domain,
                                    dssResult.exitCode,
                                    dssResult.consoleLog
                                )
                            )

                            finishCounter += 1
                            logger.info("dss scan: finished $finishCounter from ${domains.size}")
                        }
                    }
                }
            }
        }
        logger.info("finished dss scan")

        runBlocking {
            var finishCounter = 0
            var startCounter = 0

            logger.info("starting dane mx scan")

            mxRecordsNotNull.forEach { mxRecord ->
                launch(Dispatchers.IO) {
                    semaphore.withPermit {
                        mutex.withLock {
                            startCounter += 1
                            logger.info("dane mx scan: started $startCounter from ${mxRecordsNotNull.size}")
                        }

                        val daneCommandResult = testDane(mxRecord, rateLimiterDane)

                        mutex.withLock {
                            daneMxResults.add(
                                DaneCheckResult(
                                    mxRecord,
                                    daneCommandResult.exitCode,
                                    daneCommandResult.console_output
                                )
                            )

                            finishCounter += 1
                            logger.info("dane mx scan: finished $finishCounter from ${mxRecordsNotNull.size}")
                        }
                    }
                }
            }
        }
        logger.info("finished dane mx scan")

        val scanResult = ScanResult(
            scanTime,
            scanData,
            testSSLResults,
            dssResults,
            daneMxResults
        )

        val jsonFormatter = Json { prettyPrint = true }
        val scanResultJSON = jsonFormatter.encodeToString(ScanResult.serializer(), scanResult)
        writeJSONFile(scanResultJSON, "./scanner-result/"+ fileName +"_scanResult.json")
        logger.info("finished scan result from $fileName")

        val minutes: Int = Config["DATASET_DELAY_MINUTES"]
        for (i in 0 until minutes) {
            logger.info("waiting $minutes minutes before starting next scan - ${minutes - i} minutes left")
            Thread.sleep(1000 * 60)
        }
    }

    logger.info("total scan time: ${"%.2f".format((System.currentTimeMillis() - startTime) / 1000.0)} seconds")
}

fun writeJSONFile(content: String, fileName: String) {
    val outFile = File(fileName)
    outFile.writeText(content)
}

fun testDane(mx: String, rateLimiter: RateLimiter): DaneCommandResult {
    rateLimiter.acquire()
    return DaneCommand(mx).run(Config["DANE_TIMEOUT"])
}

suspend fun testTls(mxrecord: String, ipaddress: String, rateLimiter: RateLimiter, retries: Int): TlsCommandResult {
    var last: TlsCommandResult? = null
    return try {
        (0 until retries).asFlow().map {
            rateLimiter.acquire()
            last = TlsCommand(mxrecord, ipaddress).run(Config["TLS_TIMEOUT"])
            last!!
        }.first { it.exitCode == 0 }
    } catch (ex: NoSuchElementException) {
        val logger = LoggerFactory.getLogger(Application::class.java)
        logger.warn("testTls did not finish after $retries retries for $ipaddress")

        return last!!
    }
}

suspend fun testDss(domain: String, rateLimiter: RateLimiter, retries: Int): DssCommandResult {
    var last: DssCommandResult? = null
    return try {
        (0 until retries).asFlow().map {

            rateLimiter.acquire()
            last = DSSCommand(domain).run(Config["TLS_TIMEOUT"])
            last!!
        }.first { it.exitCode == 0 }
    } catch (ex: NoSuchElementException) {
        val logger = LoggerFactory.getLogger(Application::class.java)
        logger.warn("testDss did not finish after $retries retries for $domain")

        return last!!
    }
}
