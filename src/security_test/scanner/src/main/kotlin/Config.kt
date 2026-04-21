import org.slf4j.LoggerFactory

class Config private constructor() {

    data class ConfigValue (val configValue: String, val fromString: (String) -> Any)

    val env : MutableMap<String, ConfigValue> = mutableMapOf()
    val logger = LoggerFactory.getLogger(Config::class.java)

    fun addValue(key: String, default: String, fromString: (String) -> Any) {
        val sysval = System.getenv(key)
        val coalesced = sysval ?: default
        // Could be dangerous, would print passwords
        logger.info("loaded variable $key from environment. received value: $sysval. using value: $coalesced")
        env[key] = ConfigValue(coalesced, fromString)
    }

    fun <T> getValue(key: String): T {
        val confval = env[key]
        return confval!!.fromString(confval.configValue) as T
    }

    init {
        addValue("LOG_LEVEL", "INFO") { it }

        addValue("INPUT_FILE", "crawler.json") { it }

        addValue("DNS_NAMESERVER","8.8.8.8") { it }
        addValue("DNS_QUERIES_PER_SECOND","10.0") { it.toDouble() }
        addValue("DNS_THREAD_MULTIPLIER","4") { it.toInt() }

        addValue("TESTSSL_STARTTLS_SLEEP","60") { it.toInt() }
        addValue("TESTSSL_CONNECT_TIMEOUT","30s") { it }
        addValue("TESTSSL_OPENSSL_TIMEOUT","30s") { it }

        addValue("SCAN_THREAD_MULTIPLIER","4") { it.toInt() }

        addValue("DANE_RETRIES","5") { it.toInt() }
        addValue("DANE_TRIES_PER_SECOND","10.0") { it.toDouble() }
        addValue("DANE_TIMEOUT","300") { it.toInt() }

        addValue("TLS_RETRIES","5") { it.toInt() }
        addValue("TLS_TRIES_PER_SECOND","10.0") { it.toDouble() }
        addValue("TLS_TIMEOUT","300") { it.toInt() }

        addValue("DSS_TRIES_PER_SECOND","10.0") { it.toDouble() }

        addValue("DATASET_DELAY_MINUTES", "5") { it.toInt() }
    }

    companion object {
        private var instance : Config? = null

        operator fun <T> get(key: String) : T {
            if (instance == null)
                instance = Config()
            return instance!!.getValue(key)
        }
    }

}
