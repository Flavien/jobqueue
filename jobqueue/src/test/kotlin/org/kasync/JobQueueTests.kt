package org.kasync

import io.kotest.matchers.booleans.shouldBeFalse
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

    @Test
    fun submit_cancelInnerJob(): Unit = runBlocking {
        val jobQueue = jobQueue()

        val jobs = jobQueue.submitAll(3) {
            if (it == 2) {
                cancel()
            }
            it.toString()
        }
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, true, false)
        results.shouldMatchEach(
            { it shouldBe success("1") },
            { it should beException<CancellationException>() },
            { it shouldBe success("3") },
        )
    }

    @Test
    fun submit_cancelOuterJob(): Unit = runBlocking {
        val jobQueue = jobQueue()

        val gate = Job()
        val jobs = jobQueue.submitAll(3) {
            gate.join()
            it.toString()
        }
        jobs[1].cancel()
        gate.complete()
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, true, false)
        results.shouldMatchEach(
            { it shouldBe success("1") },
            { it should beException<CancellationException>() },
            { it shouldBe success("3") },
        )
    }

    @Test
    fun submit_failedJob(): Unit = runBlocking {
        val jobQueue = jobQueue()

        val jobs = jobQueue.submitAll(3) {
            if (it == 2) {
                throw ArithmeticException()
            }
            it.toString()
        }
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, true, false)
        results.shouldMatchEach(
            { it shouldBe success("1") },
            { it should beException<ArithmeticException>() },
            { it shouldBe success("3") },
        )
    }

    @Test
    fun submit_cancelledQueue(): Unit = runBlocking {
        val jobQueue = jobQueue()

        val jobs = jobQueue.submitAll(3) {
            if (it == 2) {
                jobQueue.cancel()
            }
            it.toString()
        }
        val results: List<Result<String>> = awaitAll(jobs)

        jobs.shouldHaveCancelledJob(false, true, true)
        results.shouldMatchEach(
            { it shouldBe success("1") },
            { it should beException<CancellationException>() },
            { it should beException<CancellationException>() },
        )
    }

    @Test
    fun submit_afterQueueCancelled(): Unit = runBlocking {
        val jobQueue = jobQueue()
        jobQueue.cancel()

        val jobs = jobQueue.submitAll(3) {
            it.toString()
        }
        val results: List<Result<String>> = awaitAll(jobs)

        jobs.shouldHaveCancelledJob(true, true, true)
        results.shouldMatchEach(
            { it should beException<CancellationException>() },
            { it should beException<CancellationException>() },
            { it should beException<CancellationException>() },
        )
    }

    @Test
    fun submit_capacityExceeded(): Unit = runBlocking {
        val jobQueue = jobQueue(1)
        val gate = Job()
        var hasRun = false

        jobQueue.submit {
            gate.join()
        }

        assertThrows<IllegalStateException>("The JobQueue is at full capacity") {
            jobQueue.submit {
                hasRun = true
            }
        }
        jobQueue.cancel()

        hasRun.shouldBeFalse()
    }

    private fun JobQueue.submitAll(
        count: Int,
        block: suspend CoroutineScope.(Int) -> String
    ): List<Deferred<String>> {
        return (1..count)
            .map { submit { block(it) } }
    }
}
