package com.holi.spark

import com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals
import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.holi.java.content
import com.holi.java.plusAssign
import com.holi.java.toList
import com.holi.spark.hamkrest.hasSize
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

@Suppress("MemberVisibilityCanPrivate")
class SparkResilientDistributedDatasetTest : SharedJavaSparkContext() {

    @get:Rule val folder: TemporaryFolder = TemporaryFolder()
    val lines = listOf("#Spark", "Spark Resilient Distributed Dataset test")
    val readme by lazy {
        folder.newFile("test")!!.apply {
            writer().use { it += lines.joinToString("\n") }
        }
    }

    val RDD by lazy { jsc().textFile(readme.path) }

    @Test
    fun `create resilient distributed dataset from file`() {
        assert.that(RDD, hasSize(lines.size))
        assert.that(RDD.toLocalIterator().toList(), equalTo(lines))
    }


    @Test
    fun `create resilient distributed dataset from directory`() {
        assertRDDEquals(RDD, jsc().textFile(readme.parent))
    }

    @Test
    fun `fetch the first 'item' from resilient distributed dataset`() {
        assert.that(RDD.first(), equalTo(lines[0]))
    }


    @Test
    fun `create resilient distributed dataset from parallelized list`() {
        val it = jsc().parallelize(lines + "additional")

        assert.that(it, hasSize(lines.size + 1))
    }

    @Test
    fun `create resilient distributed dataset from directory contains multiple small files with 'filename' and 'content' attributes`() {
        val it = jsc().wholeTextFiles(readme.parent)

        val (filename, content) = it.first()
        assert.that(it, hasSize(1))
        assert.that(filename, equalTo(readme.toURI().toString()))
        assert.that(content, equalTo(readme.content))
    }

    @Test
    @Ignore("How to create a sequence file?")
    fun `create resilient distributed dataset from sequence file which contains 'key' and 'value'`() {
        val expected = mapOf("foo" to 1, "bar" to 2)
        val sequence = folder.newFile("sequence").apply {
            writer().use {
                it += expected.asSequence().mapIndexed { index, (key, value) -> "$index.$key:$value" }.joinToString("\n")
            }
        }

        val it = jsc().sequenceFile(sequence.path, String::class.java, Int::class.java)

        assert.that(it, hasSize(2))
        assert.that(it.collectAsMap(), equalTo(expected))
    }
}

