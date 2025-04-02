package org.kasync

import kotlinx.coroutines.*
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class PartitionedJobQueue(
    coroutineContext: CoroutineContext,
    channelCount: Int
) {
    private val context: CoroutineContext = coroutineContext + Job(coroutineContext.job)
    private val queues = List(channelCount) {
        JobQueue(context, channelCount)
    }

    fun <T> submit(
        key: Any,
        context: CoroutineContext = EmptyCoroutineContext,
        block: suspend CoroutineScope.() -> T
    ): Deferred<T> {
        val queue = queues[Math.floorMod(key.hashCode(), queues.size)]
        return queue.submit(context, block)
    }

    fun cancel() = context.cancel()
}
