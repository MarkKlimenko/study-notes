package com.example.java_example

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

//@SpringBootApplication
class Application

fun main(args: Array<String>) = runBlocking<Unit> {
    //runApplication<Application>(*args)

    withContext(Dispatchers.Default) {
        //async { KafkaExample().startKafkaProducerWithoutId() }
        async { KafkaExample().startKafkaProducerWithId() }
        async { KafkaExample().startKafkaConsumer() }
    }
}
