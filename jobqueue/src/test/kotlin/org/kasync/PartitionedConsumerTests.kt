package org.kasync

import io.kotest.assertions.nondeterministic.eventually
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.collections.shouldMatchEach
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.types.shouldBeTypeOf
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.time.Duration.Companion.seconds

class PartitionedConsumerTests {
    private val acknowledgements = mutableListOf<Pair<Int, Throwable?>>()
    private val processedMessages = Collections.synchronizedList(mutableListOf<Int>())

    @Test
    fun consume_queueJobs(): Unit = runBlocking {
        val consumer = createConsumer<Int>(5, 3) { value, throwable ->
            acknowledgements.add(value to throwable)
        }

        val job = testSource().take(10).dispatch(consumer) {
            processedMessages.add(it)
            delay(100)
        }

        job.join()
        consumer.stop()

        job.isCancelled.shouldBeFalse()
        processedMessages shouldHaveSize 10
        processedMessages.slice(0..4) shouldContainExactlyInAnyOrder (0..4).toList()
        processedMessages.slice(5..9) shouldContainExactlyInAnyOrder (5..9).toList()
        acknowledgements shouldContainExactly (0..9).map { it to null }
    }

    @Test
    fun consume_multipleSubscriptions(): Unit = runBlocking {
        val consumer = createConsumer<Int>(3, 3) { value, throwable ->
            acknowledgements.add(value to throwable)
        }

        val jobs: List<Job> = (1..3)
            .map { index ->
                testSource()
                    .onEach { delay(100) }
                    .map { 10 * index + it.first to (it.second + index) }
                    .take(3)
                    .dispatch(consumer) {
                        processedMessages.add(it)
                    }
            }

        jobs.forEach { it.join() }
        consumer.stop()

        processedMessages shouldHaveSize 9
        processedMessages.slice(0..2) shouldContainExactlyInAnyOrder listOf(10, 20, 30)
        processedMessages.slice(3..5) shouldContainExactlyInAnyOrder listOf(11, 21, 31)
        processedMessages.slice(6..8) shouldContainExactlyInAnyOrder listOf(12, 22, 32)
        acknowledgements shouldContainExactlyInAnyOrder
            listOf(10, 11, 12, 20, 21, 22, 30, 31, 32).map { it to null }
    }

    @Test
    fun consume_cancelDuringCollection(): Unit = runBlocking {
        val consumer = createConsumer<Int>(5, 3) { value, throwable ->
            acknowledgements.add(value to throwable)
        }

        val job = testSource().dispatch(consumer) {
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
    fun consume_processingError(): Unit = runBlocking {
        val consumer = createConsumer<Int>(3, 5) { value, throwable ->
            acknowledgements.add(value to throwable)
        }

        val job = testSource().take(6).dispatch(consumer) {
            processedMessages.add(it)
            if (it == 1) {
                throw ArithmeticException()
            }
        }

        job.join()
        consumer.stop()

        job.isCancelled.shouldBeFalse()
        processedMessages.slice(0..2) shouldContainExactlyInAnyOrder (0..2).toList()
        processedMessages.slice(3..5) shouldContainExactlyInAnyOrder (3..5).toList()
        acknowledgements.shouldMatchEach(
            { it.second.shouldBeNull() },
            { it.second.shouldBeTypeOf<ArithmeticException>() },
            { it.second.shouldBeNull() },
            { it.second.shouldBeNull() },
            { it.second.shouldBeNull() },
            { it.second.shouldBeNull() },
        )
    }

    private fun testSource() = flow {
        var i = 0
        while (true) {
            emit(i to i)
            i++
        }
    }
}
