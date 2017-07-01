package com.holi.spark.operations

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.holi.util.Counter
import com.natpryce.hamkrest.*
import com.natpryce.hamkrest.assertion.assert
import org.apache.spark.SparkDriverExecutionException
import org.apache.spark.SparkException
import org.apache.spark.storage.StorageLevel
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.Serializable


@Suppress("MemberVisibilityCanPrivate", "UNUSED")
class SparkRDDCachingTest : SharedJavaSparkContext(), Serializable {

    @field:Transient @get:Rule val folder: TemporaryFolder = TemporaryFolder()
    val lines = listOf("#Spark", "Spark Resilient Distributed Dataset test")

    val RDD by lazy { jsc().parallelize(lines, lines.size)!! }

    val counter = Counter.starts(0)

    @Test
    fun `caching the transformation result on resilient distributed dataset`() {
        val transformed = RDD.map { counter.inc() }

        transformed.cache()
        assert.that(counter.value, equalTo(0))

        transformed.count()
        assert.that(counter.value, equalTo(lines.size))

        transformed.count()
        assert.that(counter.value, equalTo(lines.size))
    }


    @Test
    fun `persist the transformation result on resilient distributed dataset`() {
        val transformed = RDD.map {
            counter.inc()
            return@map 1// don't let map transformation return an un-serializable Unit
        }

        transformed.persist(StorageLevel.DISK_ONLY())
        assert.that(counter.value, equalTo(0))

        transformed.count()
        assert.that(counter.value, equalTo(lines.size))

        transformed.count()
        assert.that(counter.value, equalTo(lines.size))
    }


    @Test
    fun `caching repeatedly`() {
        val it = RDD.cache()

        assert.that(it, !sameInstance(RDD))
        assert.that(it.cache(), !sameInstance(it))
    }

    @Test
    fun `doesn't re-computes the original transformation if transformed resilient distributed dataset is lost`() {
        val original = RDD.map { it.also { counter.inc() } }.cache().apply { count() }
        assert.that(counter.value, equalTo(lines.size))

        assert.that({ original.reduce { _, _ -> throw IllegalStateException() } }, throws(isA<SparkDriverExecutionException>(has(Throwable::cause, present()))))
        assert.that(counter.value, equalTo(lines.size))

        original.count()
        assert.that(counter.value, equalTo(lines.size))
    }

    @Test
    fun `re-computes the original transformation if a partition resilient distributed dataset is lost`() {
        jsc().setLogLevel("OFF")

        val original = RDD.map { failLastOnce(it) }.cache()


        assert.that({ original.count() }, throws(isA<SparkException>(has("cause", { it.cause!! }, isA<IllegalStateException>()))))
        assert.that(counter.value, equalTo(lines.size))

        original.count()
        assert.that(counter.value, equalTo(lines.size + 1))
        //                                              ^
        // fault-tolerant: re-computes the last failed operation
    }

    private fun <T> failLastOnce(value: T): T = when (Counter.connect().inc()) {
        lines.size -> throw IllegalStateException()
        else -> value
    }

}