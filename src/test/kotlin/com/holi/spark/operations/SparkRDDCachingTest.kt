package com.holi.spark.operations

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.sameInstance
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

    val counter by lazy { @Suppress("DEPRECATION") jsc().accumulator(0)!! }
    val count get() = counter.value()!!

    @Test
    fun `caching the transformation result on resilient distributed dataset`() {
        val transformed = RDD.map { counter.add(1) }

        transformed.cache()
        assert.that(count, equalTo(0))

        transformed.count()
        assert.that(count, equalTo(lines.size))

        transformed.count()
        assert.that(count, equalTo(lines.size))
    }


    @Test
    fun `persist the transformation result on resilient distributed dataset`() {
        val transformed = RDD.map {
            counter.add(1)
            return@map 1// don't let map transformation return an un-serializable Unit
        }

        transformed.persist(StorageLevel.DISK_ONLY())
        assert.that(count, equalTo(0))

        transformed.count()
        assert.that(count, equalTo(lines.size))

        transformed.count()
        assert.that(count, equalTo(lines.size))
    }


    @Test
    fun `caching repeatedly`() {
        val it = RDD.cache()

        assert.that(it, !sameInstance(RDD))
        assert.that(it.cache(), !sameInstance(it))
    }
}