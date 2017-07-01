package com.holi.util

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.hasSize
import org.junit.Test
import java.util.concurrent.Callable
import java.util.concurrent.Executors

class CounterTest {

    @Test
    fun `increment with initial value`() {
        assert.that(Counter.starts(2).inc(), equalTo(3))
    }

    @Test
    fun `share counter between jvm`() {
        assert.that(Counter.starts(0).inc(), equalTo(1))
        assert.that(Counter.connect().inc(), equalTo(2))
    }

    @Test
    fun `get value after increment`() {
        val counter = Counter.starts(1000)

        counter.inc()

        assert.that(counter.value, equalTo(1001))
    }

    @Test
    fun `increment counter synchronized`() {
        val counter = Counter.starts(0)
        val executor = Executors.newFixedThreadPool(20)

        val result: Set<Int> = (1..100).map { executor.submit(Callable { Counter.connect().inc() }) }.map { it.get() }.toSet()

        assert.that(result, hasSize(equalTo(100)))
        assert.that(counter.value, equalTo(result.size))
    }
}