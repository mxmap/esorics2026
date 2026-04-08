import org.slf4j.LoggerFactory

class Config private constructor() {

    data class ConfigValue (val configValue: String, val fromString: (String) -> Any)

    val env : MutableMap<String, ConfigValue> = mutableMapOf()
    val logger = LoggerFactory.getLogger(Config::class.java)

    fun addValue(key: String, default: String, fromString: (String) -> Any) {
        val sysval = System.getenv(key)
        val coalesced = sysval ?: default
        // Could be dangerous, would print passwords
        // logger.info("loaded variable $key2 from environment. received value: $sysval. using value: $coalesced")
        env[key] = ConfigValue(coalesced, fromString)
    }

    fun <T> getValue(key: String): T {
        val confval = env[key]
        return confval!!.fromString(confval.configValue) as T
    }

    init {
        addValue("LOG_LEVEL", "INFO") { it }
    }

    companion object {
        private var instance : Config? = null

        operator fun <T> get(key: String) : T {
            if (instance == null)
                instance = Config()
            return instance!!.getValue(key)
        }

        fun preload() {
            if (instance == null)
                instance = Config()
        }
    }

}
