package com.holi.util

import java.io.*
import java.lang.Runtime.getRuntime
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import kotlin.concurrent.thread


private const val INC = 1;
private const val RESET = 2;
private const val VALUE = 3;

interface Counter : Serializable {
    fun inc(): Int;
    val value: Int;

    companion object {
        private const val SERVER_PORT = 12345;

        @Synchronized fun starts(initial: Int): Counter {
            return connection(initial) { create(initial).let { connect() } }
        }

        private fun create(initial: Int) {
            ServerSocket(SERVER_PORT).apply {
                apply { getRuntime().addShutdownHook(thread(false) { close() }) }
                run {
                    thread(isDaemon = true) {
                        var value = initial;
                        while (true) {
                            try {
                                value = accept().serve(value)
                            } catch(ex: SocketException) {
                                if (isClosed) break
                            }
                        }
                    }
                }
            }
        }

        public fun connect(): Counter {
            return connection().also { it.value }
        }

        private fun connection(identity: Int? = null, exceptionally: (Throwable) -> Counter = { throw it }): Counter {
            try {
                return object : Counter {
                    init {
                        identity?.let { reset(it) }
                    }

                    private fun reset(value: Int): Int = send(RESET, value)

                    override fun inc() = send(INC)

                    override val value get() = send(VALUE)
                }
            } catch(ex: Throwable) {
                return exceptionally(ex)
            }

        }

        private fun send(command: Int, value: Int = 0): Int {
            Socket(InetAddress.getLocalHost(), SERVER_PORT).configured.use {
                ObjectOutputStream(it.getOutputStream()).let {
                    it.writeInt(command)
                    it.writeInt(value)
                    it.flush()
                }
                return DataInputStream(it.getInputStream()).let { it.readInt() };
            }
        }


    }
}

private val Socket.configured: Socket get() {
    soTimeout = 1000
    return this;
}

private fun Socket.serve(value: Int): Int {
    return configured.use {
        run {
            var result = value
            ObjectInputStream(getInputStream()).let {
                when (it.readInt()) {
                    INC -> result += 1
                    RESET -> result = it.readInt()
                }
            }
            DataOutputStream(getOutputStream()).let {
                it.writeInt(result)
                it.flush()
            }
            return@run result;
        }
    }
}

private inline fun <T : Closeable, R> T.use(block: (T) -> R): R {
    try {
        return block(this);
    } finally {
        try {
            close()
        } catch (ignored: IOException) {
        }
    }
}