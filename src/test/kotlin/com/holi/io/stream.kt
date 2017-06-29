package com.holi.io

import java.lang.Appendable

@Suppress("NOTHING_TO_INLINE")
inline operator fun Appendable.plusAssign(text: String) {
    append(text)
}