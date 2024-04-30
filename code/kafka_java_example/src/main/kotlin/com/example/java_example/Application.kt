package com.example.java_example

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext

class Application

fun main(args: Array<String>) = runBlocking<Unit> {
    //withContext(Dispatchers.Default) {
    //    //async { KafkaExample().startKafkaProducerWithoutId() }
    //    launch { KafkaExample().startKafkaProducerWithId() }
    //    launch { KafkaExample().startKafkaConsumers() }
    //}

    //withContext(Dispatchers.Default) {
        doWork()
    //}
}

suspend fun doWork() {
    supervisorScope {
        launch {
            println("BLAAAAAA")
            Thread.sleep(50000)
        }

        launch {
            println("AAAAAABL")
        }
    }
}
