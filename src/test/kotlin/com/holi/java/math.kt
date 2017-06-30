package com.holi.java

import java.util.concurrent.atomic.AtomicInteger


operator fun AtomicInteger.unaryPlus() {
    incrementAndGet()
}