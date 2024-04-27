package com.example.java_example

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import kotlin.random.Random

/*
  kafka-topics --bootstrap-server broker:9092 \
   --topic demo_java --create --partitions 3 --replication-factor 1

  kafka-console-consumer --bootstrap-server broker:9092 \
   --topic demo_java
 */
class KafkaExample {
    private val log: Logger = LoggerFactory.getLogger(KafkaExample::class.java)

    fun startKafkaProducerWithoutId() {
        log.info("Start kafka producer")

        val producer: KafkaProducer<String, String> = getProducer()

        repeat(10) {
            val producerRecord = ProducerRecord<String, String>("demo_java", "hello world")

            // send data - asynchronous
            producer.send(producerRecord) { metadata: RecordMetadata, e: Exception? ->
                if (e == null) {
                    log.info(
                        """ 
                            Received new metadata. 
                            Topic:${metadata.topic()}
                            Partition: ${metadata.partition()}
                            Offset: ${metadata.offset()}
                            Timestamp: ${metadata.timestamp()}
                        """.trimIndent()
                    );
                } else {
                    log.error("Error while producing", e);
                }
            }

            Thread.sleep(1000)
        }

        // flush data - synchronous
        producer.flush()

        // flush and close producer
        producer.close()
    }

    /**
     * The messages are sent in batches by partition, which is why you see all the messages of Partition 1 first
     * (first one that was sent), then message of Partition 0 then (second one that was sent) and then Partition 2
     * (last one)
     *
     * If you re-run the code, you will find that the message with the same key will go to
     * the same partition (try re-running the code and compare your outputs!)
     */
    fun startKafkaProducerWithId() {
        log.info("Start kafka producer with id")

        val producer: KafkaProducer<String, String> = getProducer()

        while (true) {
            val id = Random.nextInt(0, 2000).toString()

            val producerRecord = ProducerRecord(
                "demo_java",
                id.toString(),
                "hello world $id"
            )

            // send data - asynchronous
            producer.send(producerRecord) { metadata: RecordMetadata, e: Exception? ->
                if (e == null) {
                    log.info(
                        "PRODUCER " +
                            "Key:${id.padEnd(4)} " +
                            "Partition: ${metadata.partition()} " +
                            "Offset: ${metadata.offset().toString().padEnd(5)} " +
                            "Timestamp: ${metadata.timestamp()}"
                    );
                } else {
                    log.error("Error while producing", e);
                }
            }

            Thread.sleep(200)
        }

        // flush data - synchronous
        producer.flush()

        // flush and close producer
        producer.close()
    }

    private fun getProducer(): KafkaProducer<String, String> {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092")
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        return KafkaProducer<String, String>(properties)
    }

    fun startKafkaConsumer() {
        log.info("Start kafka consumer")

    }
}
