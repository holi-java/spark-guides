package com.holi.java

import java.io.File
import java.lang.Appendable

@Suppress("NOTHING_TO_INLINE")
inline operator fun Appendable.plusAssign(text: String) {
    append(text)
}

val File.content get() = readText()