package org.kasync

import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job

suspend fun awaitAll(jobs: List<Deferred<String>>): List<Result<String>> {
    jobs.last().join()
    return jobs.map {
        runCatching { it.await() }
    }
}

inline fun <reified T : Throwable> beException() = Matcher<Result<Any>> { value ->
    val exception = value.exceptionOrNull()
    MatcherResult(
        exception != null && exception is T,
        { "Result should be a failure of type ${T::class.java} but was $value" },
        { "Result should not be a failure of type ${T::class.java} but was $value" },
    )
}

fun List<Job>.shouldHaveCancelledJob(vararg cancelled: Boolean) {
    map { it.isCancelled } shouldBe cancelled
    forEach { it.isActive.shouldBeFalse() }
    forEach { it.isCompleted.shouldBeTrue() }
}
