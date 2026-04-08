import models.DigResult
import java.io.File
import java.util.*

object Dig {
    fun createTmpPath(): String {
        return "/tmp/${"dig"}_${UUID.randomUUID()}"
    }

    fun dig(domain: String, type: String): DigResult {
        val logfilepath = createTmpPath()

        val pb =
            ProcessBuilder("timeout", "30", "dig", "@" + Config["DNS_NAMESERVER"], domain, type)
        val logfile = File(logfilepath)
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logfile))
        pb.redirectError(ProcessBuilder.Redirect.appendTo(logfile))
        val exitcode = pb.start().waitFor()

        val log = File(logfilepath).readText(Charsets.UTF_8)

        val logLines = log.split("\n")

        val adFlag = logLines.find { it.startsWith(";; flags:") }?.contains("ad") == true
        val beginAnswerSection = logLines.indexOf(";; ANSWER SECTION:")
        if (beginAnswerSection != -1) {
            val answerSection =
                logLines.filterIndexed { index, s -> index > beginAnswerSection && s.isNotBlank() }

            val answers = answerSection.filter { it.split("\t", " ").any { it == type } }.map {
                val answer = it.split("\t", " ").last().lowercase()
                if (type == "MX") answer.dropLast(1) else answer
            }
            return DigResult(answers, adFlag)
        }
        return DigResult(emptyList(), adFlag)
    }

}