package processes

import Config
import org.slf4j.LoggerFactory
import java.io.File

class TlsCommand(private val mxrecord: String, private val ipaddress: String) : Command("tls") {

    fun run(timeout: Int): TlsCommandResult {
        val logger = LoggerFactory.getLogger(TlsCommand::class.java)
        val logfilepath = createTmpPath()

        val pb = ProcessBuilder(
            "timeout",
            timeout.toString(),
            "/app/testssl/testssl.sh",
            "--quiet",
            "--connect-timeout",
            Config.get<String>("TESTSSL_CONNECT_TIMEOUT"),
            "--openssl-timeout",
            Config.get<String>("TESTSSL_OPENSSL_TIMEOUT"),
            "--debug",
            "2",
            "--color",
            "0",
            "-p",
            "-t",
            "smtp",
            "-n",
            "none",
            "--ip",
            ipaddress,
            "${mxrecord}:25"
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

        return TlsCommandResult(exitcode, log)
    }
}
