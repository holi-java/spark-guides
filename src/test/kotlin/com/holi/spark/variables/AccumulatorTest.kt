@file:Suppress("MemberVisibilityCanPrivate", "HasPlatformType")

package com.holi.spark.variables

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.holi.spark.disableLog
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.isA
import com.natpryce.hamkrest.throws
import org.apache.spark.SparkException
import org.apache.spark.util.AccumulatorV2
import org.junit.Test
import java.io.NotSerializableException
import java.io.Serializable

class AccumulatorTest : SharedJavaSparkContext(), Serializable {
    val RDD by lazy { jsc().parallelize(listOf(1, 2, 3)) }
    val sum by lazy { @Suppress("DEPRECATION") jsc().accumulator(0) }

    @Test
    fun `adding the value in an accumulator`() {
        RDD.map { sum.add(it).let { 1 } }.count()

        assert.that(sum.value(), equalTo(6));
    }

    @Test
    fun `can't read the value during the parallel operations`() {
        jsc().disableLog()

        assert.that(
                { RDD.reduce { _, _ -> sum.value() } },
                throws(isA<SparkException>(has("cause", { it.cause!! }, isA<UnsupportedOperationException>())))
        )
    }


    @Test
    fun `register customized accumulator before to used in operations`() {
        val params = Parameters().also { jsc().sc().register(it) }

        RDD.takeOrdered(3).reduce { first, second -> params.add(first to second).let { -second } }

        assert.that(params.value(), equalTo(-1 to 5))
    }
}

data class Parameters(var pair: Pair<Int, Int>? = null) : AccumulatorV2<Pair<Int, Int>, Pair<Int, Int>>(), Serializable {
    override fun isZero(): Boolean = pair?.let { false } ?: true

    override fun reset() = let { pair = null }

    override fun add(added: Pair<Int, Int>?) {
        pair = pair?.run { Pair(first + (added?.first ?: 0), second + (added?.second ?: 0)) } ?: added
    }

    override fun copy(): AccumulatorV2<Pair<Int, Int>, Pair<Int, Int>> {
        return this.copy(pair)
    }

    override fun merge(addend: AccumulatorV2<Pair<Int, Int>, Pair<Int, Int>>?) {
        add(addend?.value())
    }

    override fun value(): Pair<Int, Int>? = pair
}