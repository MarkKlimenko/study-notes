package com.example.java_example

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

/*
  kafka-topics --bootstrap-server broker:9092 \
   --topic demo_java --create --partitions 3 --replication-factor 1

  kafka-console-consumer --bootstrap-server broker:9092 \
   --topic demo_java
 */
class KafkaExample {
    private val log: Logger = LoggerFactory.getLogger(KafkaExample::class.java)

    fun startKafkaPublisher() {

        log.info("Start kafka producer")

        val bootstrapServers = "127.0.0.1:29092"

        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        val producer = KafkaProducer<String, String>(properties)

        val producerRecord = ProducerRecord<String, String>("demo_java", "hello world")

        // send data - asynchronous
        producer.send(producerRecord)

        // flush data - synchronous
        producer.flush()

        // flush and close producer
        producer.close()
    }
}
