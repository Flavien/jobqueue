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
            timestamps[2 * it] = Instant.now()
            delay(50)
            timestamps[2 * it + 1] = Instant.now()
        }
        jobs.reversed().forEach { it.join() }
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
            if (it == 1) {
                cancel()
            }
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
            if (it == 1) {
                throw ArithmeticException()
            }
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
    fun submit_cancelQueue(): Unit = runBlocking {
        val jobQueue = jobQueue()

        val jobs = jobQueue.submitAll(3) {
            if (it == 1) {
                jobQueue.cancel()
            }
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
    fun submit_cancelScope(): Unit = runBlocking{
        val scopeJob = Job()
        val jobsFuture: CompletableDeferred<List<Deferred<String>>> = CompletableDeferred()

        launch(scopeJob) {
            val jobQueue = jobQueue()

            val jobs = jobQueue.submitAll(3) {
                if (it == 1) {
                    scopeJob.cancel()
                }
            }
            jobsFuture.complete(jobs)
        }
        val jobs = jobsFuture.await()
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

        val jobs = jobQueue.submitAll(3) { }
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
        var hasRun = false

        jobQueue.submit {
            Job().join()
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
        block: suspend CoroutineScope.(Int) -> Unit
    ): List<Deferred<String>> {
        return (0..<count).map {
            submit {
                block(it)
                (it + 1).toString()
            }
        }
    }
}
