package org.kasync

import io.kotest.matchers.Matcher
import io.kotest.matchers.MatcherResult

inline fun <reified T : Throwable> beException() = Matcher<Result<Any>> { value ->
    val exception = value.exceptionOrNull()
    MatcherResult(
        exception != null && exception is T,
        { "Result should be a failure of type ${T::class.java} but was $value" },
        { "Result should not be a failure of type ${T::class.java} but was $value" },
    )
}
