package org.kasync

import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.lastOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext

@OptIn(ExperimentalCoroutinesApi::class)
class PartitionedConsumer<T>(
    coroutineContext: CoroutineContext,
    channelCount: Int,
    inputQueueCapacity: Int,
    private val ack: suspend (T, Throwable?) -> Unit,
) {
    private val scope = CoroutineScope(coroutineContext + Job())
    private val jobQueue = PartitionedJobQueue(scope.coroutineContext, channelCount)
    private val inputChannel = Channel<MessageExecution<T>>(capacity = inputQueueCapacity)

    val job: Job
        get() = scope.coroutineContext.job

    init {
        scope.launch {
            for (message in inputChannel) {
                message.task.join()
                ack(message.message, message.task.getCompletionExceptionOrNull())
                message.acknowledged.complete()
            }
        }
    }

    fun consume(
        messages: Flow<Pair<T, Any>>,
        process: suspend CoroutineScope.(T) -> Unit
    ): Job = scope.launch {
        messages
            .map { (value, partition) ->
                val future = jobQueue.submit(partition) {
                    process(value)
                }
                val execution = MessageExecution(value, future, Job())
                inputChannel.send(execution)
                execution
            }
            .lastOrNull()
            ?.acknowledged
            ?.join()
    }

    fun stop() = scope.cancel()

    private data class MessageExecution<T>(
        val message: T,
        val task: Deferred<Unit>,
        val acknowledged: CompletableJob
    )
}

fun <T> CoroutineScope.createConsumer(
    channelCount: Int,
    inputQueueCapacity: Int,
    ack: suspend (T, Throwable?) -> Unit
): PartitionedConsumer<T> = PartitionedConsumer(
    coroutineContext = coroutineContext,
    channelCount = channelCount,
    inputQueueCapacity = inputQueueCapacity,
    ack = ack
)

fun <T> Flow<Pair<T, Any>>.dispatch(
    consumer: PartitionedConsumer<T>,
    process: suspend CoroutineScope.(T) -> Unit
) = consumer.consume(this, process)
