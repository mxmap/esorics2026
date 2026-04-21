package emailtest.analysis

import emailtest.models.DatabaseRow
import emailtest.models.MXRecordInfo

fun createMxRecordInfo(
    data: List<DatabaseRow>
): MXRecordInfo {
    val mxRecordCountDistinct = data.getMXRecordCount()

    return MXRecordInfo(
        mxRecordCountDistinct
    )
}

private fun List<DatabaseRow>.getMXRecordCount() =
    getDistinctCount { it.mxrecordName }

fun <T> List<DatabaseRow>.getDistinctCount(splitFunction: (DatabaseRow) -> T) =
    distinctBy { splitFunction(it) }.count()
