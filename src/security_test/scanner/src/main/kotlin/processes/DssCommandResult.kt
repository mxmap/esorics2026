package processes

data class DssCommandResult(
    val exitCode: Int,
    val consoleLog: String
)
