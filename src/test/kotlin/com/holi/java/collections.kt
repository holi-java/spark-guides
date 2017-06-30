package com.holi.java

fun <T> Iterator<T>.toList() = asSequence().toList()