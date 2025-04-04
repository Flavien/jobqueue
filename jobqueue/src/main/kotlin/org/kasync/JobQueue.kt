@file:OptIn(InternalForInheritanceCoroutinesApi::class, ExperimentalCoroutinesApi::class)

package org.kasync

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class JobQueue(
    coroutineContext: CoroutineContext,
    capacity: Int
) {
    private val channel = Channel<Job>(capacity = capacity)
    private val coroutineScope: CoroutineScope = CoroutineScope(coroutineContext + SupervisorJob(coroutineContext.job))

    init {
        val loopJob = coroutineScope.coroutineContext.job

        loopJob.invokeOnCompletion {
            channel.close()
        }

        coroutineScope.launch(Job() + CoroutineName("JobQueue")) {
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

        val channelResult = channel.trySend(future)

        when {
            channelResult.isClosed -> future.cancel()
            channelResult.isFailure -> error("The JobQueue is at full capacity")
        }

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

private class ReadOnlyDeferred<T>(source: Deferred<T>) : Deferred<T> by source

fun CoroutineScope.jobQueue(capacity: Int = Channel.UNLIMITED) = JobQueue(coroutineContext, capacity)
