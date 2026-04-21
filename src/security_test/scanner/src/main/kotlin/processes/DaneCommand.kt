package processes

import Config
import java.io.File

class DaneCommand(private val mx: String) : Command("dane") {

    fun run(timeout: Int): DaneCommandResult {
        val logfilepath = createTmpPath()

        val pb = ProcessBuilder(
            "timeout", timeout.toString(), "/app/gotls/gotls", "-d", "-4", "-m", "dane", "-s", "smtp", "-r",
            Config["DNS_NAMESERVER"], mx, "25"
        )
        val logfile = File(logfilepath)
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logfile))
        pb.redirectError(ProcessBuilder.Redirect.appendTo(logfile))
        val exitcode = pb.start().waitFor()

        val log = File(logfilepath).readText(Charsets.UTF_8)

        return DaneCommandResult(exitcode, log)
    }


}
