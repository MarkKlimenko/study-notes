package com.example.java_example

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KafkaExample {
    fun startKafkaPublisher() {
        val log: Logger = LoggerFactory.getLogger(KafkaExample::class.java)

        log.info("Hello World")
    }
}
