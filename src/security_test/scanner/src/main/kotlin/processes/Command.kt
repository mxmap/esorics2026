package processes

import java.util.*


abstract class Command(private val identifier: String) {

    protected fun createTmpPath() : String {
        return "/tmp/${identifier}_${UUID.randomUUID()}"
    }
}
