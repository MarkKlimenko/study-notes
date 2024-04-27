package com.example.java_example

//@SpringBootApplication
class Application

fun main(args: Array<String>) {
    //runApplication<Application>(*args)

    KafkaExample().startKafkaPublisher()
}
