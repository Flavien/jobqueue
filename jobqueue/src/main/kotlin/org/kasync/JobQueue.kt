package org.kasync

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.InternalForInheritanceCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

@OptIn(ExperimentalCoroutinesApi::class)
class JobQueue(
    coroutineContext: CoroutineContext,
    capacity: Int
) {
    private val channel = Channel<Job>(capacity = capacity)
    private val coroutineScope: CoroutineScope = CoroutineScope(coroutineContext + SupervisorJob(coroutineContext.job))

    init {
        coroutineScope.launch {
            for (job: Job in channel) {
                job.join()
            }
        }
    }

    fun <T> submit(
        context: CoroutineContext = EmptyCoroutineContext,
        task: suspend CoroutineScope.() -> T
    ): Deferred<T> {
        val result = CompletableDeferred<T>(coroutineScope.coroutineContext.job)
        val future: Deferred<T> = coroutineScope.async(context + result, CoroutineStart.LAZY, task)

        val channelResult: ChannelResult<Unit> = channel.trySend(future)
        check(channelResult.isSuccess) { "The JobQueue is at full capacity" }

        future.invokeOnCompletion { throwable ->
            when (throwable) {
                null -> result.complete(future.getCompleted())
                else -> result.completeExceptionally(throwable)
            }
        }

        return ReadOnlyDeferred(result)
    }

    fun cancel() = coroutineScope.cancel()
}

@OptIn(InternalForInheritanceCoroutinesApi::class)
private class ReadOnlyDeferred<T>(source: Deferred<T>) : Deferred<T> by source

fun CoroutineScope.jobQueue(capacity: Int = Channel.UNLIMITED) = JobQueue(coroutineContext, capacity)
