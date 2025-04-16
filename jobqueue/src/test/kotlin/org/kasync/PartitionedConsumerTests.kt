package org.kasync

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchEach
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.time.Duration.Companion.seconds

class PartitionedConsumerTests {
    private val acknowledgements = mutableListOf<Pair<Int, Throwable?>>()
    private val processedMessages = Collections.synchronizedList(mutableListOf<Int>())

    @Test
    fun consume_queueJobs(): Unit = withConsumer(5, 3) { consumer ->
        val job = consumer.consume(testSource().take(10)) {
            processedMessages.add(it)
            delay(100)
        }

        job.join()

        job.isCancelled.shouldBeFalse()
        processedMessages shouldHaveSize 10
        processedMessages.slice(0..4) shouldContainExactlyInAnyOrder (0..4).toList()
        processedMessages.slice(5..9) shouldContainExactlyInAnyOrder (5..9).toList()
        acknowledgements shouldContainExactly (0..9).map { it to null }
    }

    @Test
    fun consume_multipleSubscriptions(): Unit = withConsumer(3, 10) { consumer ->
        val jobs: List<Job> = (1..3)
            .map { index ->
                testSource()
                    .onEach { delay(100) }
                    .map { 10 * index + it.first to (it.second + index) }
                    .take(3)
                    .let { flow ->
                        consumer.consume(flow) {
                            processedMessages.add(it)
                        }
                    }
            }

        jobs.forEach { it.join() }

        processedMessages shouldHaveSize 9
        processedMessages.slice(0..2) shouldContainExactlyInAnyOrder listOf(10, 20, 30)
        processedMessages.slice(3..5) shouldContainExactlyInAnyOrder listOf(11, 21, 31)
        processedMessages.slice(6..8) shouldContainExactlyInAnyOrder listOf(12, 22, 32)
        acknowledgements shouldContainExactlyInAnyOrder
            listOf(10, 11, 12, 20, 21, 22, 30, 31, 32).map { it to null }
    }

    @Test
    fun consume_emptySource(): Unit = withConsumer(5, 3) { consumer ->
        val job = consumer.consume(flowOf<Pair<Int, Int>>()) {
            processedMessages.add(it)
        }

        job.join()

        job.isCancelled.shouldBeFalse()
        processedMessages shouldHaveSize 0
        acknowledgements shouldHaveSize 0
    }

    @Test
    fun consume_cancelDuringCollection(): Unit = withConsumer(5, 3) { consumer ->
        val job = consumer.consume(testSource()) {
            processedMessages.add(it)
            Job().join()
        }

        eventually(5.seconds) {
            processedMessages shouldHaveSize 5
        }
        consumer.stop()

        job.isCancelled.shouldBeTrue()
        processedMessages shouldContainExactlyInAnyOrder (0..4).toList()
        acknowledgements shouldHaveSize 0
    }

    @Test
    fun consume_processingCancelled(): Unit = withConsumer(3, 5) { consumer ->
        val job = consumer.consume(testSource().take(6)) {
            processedMessages.add(it)
            if (it == 1) {
                cancel()
            }
        }

        job.join()

        job.isCancelled.shouldBeFalse()
        processedMessages.slice(0..2) shouldContainExactlyInAnyOrder (0..2).toList()
        processedMessages.slice(3..5) shouldContainExactlyInAnyOrder (3..5).toList()
        acknowledgements.shouldHaveSingleFailure<CancellationException>(count = 6, failureIndex = 1)
    }

    @Test
    fun consume_processingError(): Unit = withConsumer(3, 5) { consumer ->
        val job = consumer.consume(testSource().take(6)) {
            processedMessages.add(it)
            if (it == 1) {
                throw ArithmeticException()
            }
        }

        job.join()

        job.isCancelled.shouldBeFalse()
        processedMessages.slice(0..2) shouldContainExactlyInAnyOrder (0..2).toList()
        processedMessages.slice(3..5) shouldContainExactlyInAnyOrder (3..5).toList()
        acknowledgements.shouldHaveSingleFailure<ArithmeticException>(count = 6, failureIndex = 1)
    }

    @Test
    fun consume_cancelScope(): Unit = runBlocking {
        val scopeJob = Job()
        val jobsFuture: CompletableDeferred<Job> = CompletableDeferred()
        launch(scopeJob) {
            val consumer = createConsumer<Int>(3, 5) { value, throwable ->
                acknowledgements.add(value to throwable)
            }
            val job = consumer.consume(testSource()) {
                processedMessages.add(it)
                if (it == 1) {
                    scopeJob.cancel()
                }
            }
            jobsFuture.complete(job)
        }
        val job = jobsFuture.await()
        job.join()

        job.isCancelled.shouldBeTrue()
        processedMessages shouldContainAll listOf(0, 1)
    }

    @Test
    fun consume_acknowledgementException(): Unit = runBlocking {
        val consumerJob = Job()
        val consumer = PartitionedConsumer<Int>(coroutineContext + consumerJob, 5, 3) { value, throwable ->
            acknowledgements.add(value to throwable)
            if (value == 2) {
                throw ArithmeticException()
            }
        }

        val job = consumer.consume(testSource()) {
            processedMessages.add(it)
        }

        job.join()

        consumerJob.isCancelled.shouldBeTrue()
        job.isCancelled.shouldBeTrue()
        consumer.job.isCancelled.shouldBeTrue()
        acknowledgements shouldContainExactly (0..2).map { it to null }
    }

    @Test
    fun dispatch_suspendsUntilCompletion(): Unit = withConsumer(3, 5) { consumer ->
        testSource()
            .take(6)
            .dispatch(consumer) {
                processedMessages.add(it)
            }

        processedMessages.slice(0..2) shouldContainExactlyInAnyOrder (0..2).toList()
        processedMessages.slice(3..5) shouldContainExactlyInAnyOrder (3..5).toList()
        acknowledgements shouldContainExactly (0..5).map { it to null }
    }

    @Test
    fun dispatch_suspendsUntilCancellation(): Unit = withConsumer(3, 5) { consumer ->
        coroutineScope {
            testSource().dispatch(consumer) {
                processedMessages.add(it)
                if (it == 1) {
                    consumer.stop()
                }
            }
        }

        processedMessages shouldContainAll listOf(0, 1)
    }

    private fun testSource() = flow {
        var i = 0
        while (true) {
            emit(i to i)
            i++
        }
    }

    private fun withConsumer(
        channelCount: Int,
        inputQueueCapacity: Int,
        block: suspend CoroutineScope.(PartitionedConsumer<Int>) -> Unit) {
        runBlocking {
            val consumer = createConsumer<Int>(channelCount, inputQueueCapacity) { value, throwable ->
                acknowledgements.add(value to throwable)
            }

            block(consumer)

            consumer.stop()
        }
    }

    private inline fun <reified T : Throwable> List<Pair<Int, Throwable?>>.shouldHaveSingleFailure(
        count: Int,
        failureIndex: Int
    ) = shouldMatchEach(
        (0..<count).map {
            if (it == failureIndex) {
                { pair -> pair.second.shouldBeInstanceOf<T>() }
            } else {
                { pair -> pair.second.shouldBeNull() }
            }
        }
    )
}
