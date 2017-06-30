package com.holi.spark.operations

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.holi.java.unaryPlus
import com.holi.spark.hamkrest.asInt
import com.holi.spark.hamkrest.hasSize
import com.natpryce.hamkrest.*
import com.natpryce.hamkrest.assertion.assert
import org.apache.spark.SparkException
import org.apache.spark.storage.StorageLevel.DISK_ONLY
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.Serializable
import java.util.concurrent.atomic.AtomicInteger

@Suppress("MemberVisibilityCanPrivate", "UNUSED")
class SparkRDDSerialzingTest : SharedJavaSparkContext(), Serializable {

    @field:Transient @get:Rule val folder: TemporaryFolder = TemporaryFolder()
    val lines = listOf("#Spark", "Spark Resilient Distributed Dataset test")

    val RDD by lazy { jsc().parallelize(lines, lines.size)!! }

    val counter by lazy { @Suppress("DEPRECATION") jsc().accumulator(0)!! }

    val count get() = counter.value()!!


    @Test
    fun `persist need all variables and methods can be serialized`() {
        jsc().setLogLevel("OFF")

        val transformed = RDD.map {
            counter.add(1)//kotlin return an un-serializable Unit value
        }

        transformed.persist(DISK_ONLY())

        assert.that({ transformed.count() }, throws(isA<SparkException>(has(SparkException::cause, absent()))))
    }

    @Test
    fun `variables must be serializable in parallel resilient distributed dataset operations`() {
        val local = AtomicInteger(0);

        RDD.foreach { counter.add(1).also { +local } }

        assert.that(local, asInt(0))
        assert.that(counter.value(), equalTo(2))
    }


    @Test
    fun `un-persisted transformation result can be un-serializable`() {
        assert.that(RDD.map { Unit }, hasSize(lines.size))
    }
}