package org.kasync

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldBeMonotonicallyIncreasing
import io.kotest.matchers.collections.shouldMatchEach
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.Result.Companion.success

class JobQueueTests {
    @Test
    fun submit_queueJobs(): Unit = runBlocking {
        val jobQueue = jobQueue()
        val timestamps = Array(6) { Instant.EPOCH }

        val jobs = jobQueue.submitAll(3) {
            timestamps[2 * (it - 1)] = Instant.now()
            delay(50)
            timestamps[2 * (it - 1) + 1] = Instant.now()
            it.toString()
        }
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, false, false)
        results shouldBe listOf(
            success("1"),
            success("2"),
            success("3")
        )
        timestamps.shouldBeMonotonicallyIncreasing()
    }

    private fun JobQueue.submitAll(
        count: Int,
        block: suspend CoroutineScope.(Int) -> String
    ): List<Deferred<String>> {
        return (1..count)
            .map { submit { block(it) } }
    }

    private suspend fun awaitAll(jobs: List<Deferred<String>>): List<Result<String>> {
        return jobs.map { runCatching { it.await() } }
    }

    private fun List<Job>.shouldHaveCancelledJob(vararg cancelled: Boolean) {
        map { it.isCancelled } shouldBe cancelled
        forEach { it.isActive.shouldBeFalse() }
        forEach { it.isCompleted.shouldBeTrue() }
    }
}
