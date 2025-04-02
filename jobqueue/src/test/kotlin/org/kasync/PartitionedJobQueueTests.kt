package org.kasync

import io.kotest.matchers.collections.shouldBeMonotonicallyIncreasing
import io.kotest.matchers.collections.shouldMatchEach
import io.kotest.matchers.date.shouldBeBefore
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.Result.Companion.success
import kotlin.coroutines.cancellation.CancellationException

class PartitionedJobQueueTests {
    @Test
    fun submit_queueJobsInSamePartition(): Unit = runBlocking {
        val jobQueue = PartitionedJobQueue(coroutineContext, 10)
        val timestamps = Array(6) { Instant.EPOCH }

        val jobs: List<Deferred<String>> = listOf(-9, 1, 11).mapIndexed { index, partition ->
            jobQueue.submit(partition) {
                timestamps[2 * index] = Instant.now()
                delay(50)
                timestamps[2 * index + 1] = Instant.now()
                index.toString()
            }
        }
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, false, false)
        results shouldBe listOf(
            success("0"),
            success("1"),
            success("2")
        )
        timestamps.shouldBeMonotonicallyIncreasing()
    }

    @Test
    fun submit_queueJobsInDifferentPartitions(): Unit = runBlocking {
        val jobQueue = PartitionedJobQueue(coroutineContext, 10)
        val startTimes = Array(3) { Instant.EPOCH }
        val endTimes = Array(3) { Instant.EPOCH }

        val jobs: List<Deferred<String>> = (0..2).map {
            jobQueue.submit(it) {
                startTimes[it] = Instant.now()
                delay(50)
                endTimes[it] = Instant.now()
                it.toString()
            }
        }
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, false, false)
        results shouldBe listOf(
            success("0"),
            success("1"),
            success("2")
        )
        startTimes.max() shouldBeBefore endTimes.min()
    }

    @Test
    fun submit_cancelledInnerJob(): Unit = runBlocking {
        val jobQueue = PartitionedJobQueue(coroutineContext, 10)

        val jobs: List<Deferred<String>> = (0..2).map {
            jobQueue.submit(it) {
                if (it == 1) {
                    cancel()
                }
                it.toString()
            }
        }
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, true, false)
        results.shouldMatchEach(
            { it shouldBe success("0") },
            { it should beException<CancellationException>() },
            { it shouldBe success("2") },
        )
    }

    @Test
    fun submit_cancelledOuterJob(): Unit = runBlocking {
        val jobQueue = PartitionedJobQueue(coroutineContext, 10)

        val gate = Job()
        val jobs: List<Deferred<String>> = (0..2).map {
            jobQueue.submit(it) {
                gate.join()
                it.toString()
            }
        }
        jobs[1].cancel()
        gate.complete()
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, true, false)
        results.shouldMatchEach(
            { it shouldBe success("0") },
            { it should beException<CancellationException>() },
            { it shouldBe success("2") },
        )
    }

    @Test
    fun submit_failedJob(): Unit = runBlocking {
        val jobQueue = PartitionedJobQueue(coroutineContext, 10)

        val jobs: List<Deferred<String>> = (0..2).map {
            jobQueue.submit(it) {
                if (it == 1) {
                    throw ArithmeticException()
                }
                it.toString()
            }
        }
        val results: List<Result<String>> = awaitAll(jobs)
        jobQueue.cancel()

        jobs.shouldHaveCancelledJob(false, true, false)
        results.shouldMatchEach(
            { it shouldBe success("0") },
            { it should beException<ArithmeticException>() },
            { it shouldBe success("2") },
        )
    }

    @Test
    fun submit_cancelledQueue(): Unit = runBlocking {
        val jobQueue = PartitionedJobQueue(coroutineContext, 10)

        jobQueue.cancel()
        val jobs: List<Deferred<String>> = (0..2).map {
            jobQueue.submit(it) {
                it.toString()
            }
        }
        val results: List<Result<String>> = awaitAll(jobs)

        jobs.shouldHaveCancelledJob(true, true, true)
        results.shouldMatchEach(
            { it should beException<CancellationException>() },
            { it should beException<CancellationException>() },
            { it should beException<CancellationException>() },
        )
    }
}
