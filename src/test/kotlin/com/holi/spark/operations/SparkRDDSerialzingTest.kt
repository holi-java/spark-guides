package com.holi.spark.operations

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.holi.java.unaryPlus
import com.holi.spark.disableLog
import com.holi.spark.hamkrest.asInt
import com.holi.spark.hamkrest.hasSize
import com.holi.util.Counter
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


    @Test
    fun `persist need all variables and methods can be serialized`() {
        jsc().disableLog()

        val transformed = RDD.map {
            return@map Unit//kotlin return an un-serializable Unit value
        }

        transformed.persist(DISK_ONLY())

        assert.that({ transformed.count() }, throws(isA<SparkException>(has(SparkException::cause, absent()))))
    }

    @Test
    fun `variables must be serializable in parallel resilient distributed dataset operations`() {
        val local = AtomicInteger(0);
        val shared = Counter.starts(0)

        RDD.foreach { shared.inc().also { +local } }

        assert.that(local, asInt(0))
        assert.that(shared.value, equalTo(2))
    }


    @Test
    fun `un-persisted transformation result can be un-serializable`() {
        assert.that(RDD.map { Unit }, hasSize(lines.size))
    }
}