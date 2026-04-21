package processes

import Config
import org.slf4j.LoggerFactory
import java.io.File

class DSSCommand(private val domain: String) : Command("dss") {

    fun run(timeout: Int): DssCommandResult {
        val logger = LoggerFactory.getLogger(DSSCommand::class.java)
        val logfilepath = createTmpPath()

        val pb = ProcessBuilder(
            "timeout",
            timeout.toString(),
            "/usr/bin/dss",
            "scan",
            domain,
            "--dkimSelector",
            "gca"
        )
        val env = pb.environment()
        env["STARTTLS_SLEEP"] = Config.get<Int>("TESTSSL_STARTTLS_SLEEP").toString()
        val logfile = File(logfilepath)
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logfile))
        pb.redirectError(ProcessBuilder.Redirect.appendTo(logfile))
        val exitcode = pb.start().waitFor()

        var log = File(logfilepath).readText(Charsets.UTF_8)
        log += "\nExitcode: $exitcode"
        if (exitcode == 124) {
            log += "\ntimed out after $timeout seconds"
        }

        return DssCommandResult(exitcode, log)
    }
}
