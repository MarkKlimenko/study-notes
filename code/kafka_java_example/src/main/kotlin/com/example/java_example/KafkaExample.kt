package com.example.java_example

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties
import kotlin.random.Random
import kotlin.time.DurationUnit.MILLISECONDS
import kotlin.time.toDuration
import kotlin.time.toJavaDuration


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
                    /*log.info(
                        "PRODUCER " +
                            "Key:${id.padEnd(4)} " +
                            "Partition: ${metadata.partition()} " +
                            "Offset: ${metadata.offset().toString().padEnd(5)} " +
                            "Timestamp: ${metadata.timestamp()}"
                    )*/
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

    suspend fun startKafkaConsumers() {
        log.info("Start kafka consumers")

        withContext(Dispatchers.Default) {
            launch { startConsumerGroup("consumer-group-1", 3) }
            launch { startConsumerGroup("consumer-group-2", 1) }
        }
    }

    private suspend fun startConsumerGroup(groupId: String, consumersCount: Int) {
        withContext(Dispatchers.Default) {
            repeat(consumersCount) {
                launch { startConsumer(groupId, it) }
            }
        }
    }

    private fun startConsumer(groupId: String, consumerId: Int) {
        val bootstrapServers = "127.0.0.1:29092"
        val topic = "demo_java"

        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        val consumer = KafkaConsumer<String, String>(properties)

        // get a reference to the current thread
        val mainThread = Thread.currentThread()

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...")
                consumer.wakeup()

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join()
                } catch (e: InterruptedException) {
                    e.printStackTrace()
                }
            }
        })

        try {
            consumer.subscribe(listOf(topic))

            while (true) {
                val records: ConsumerRecords<String, String> =
                    consumer.poll(5000.toDuration(MILLISECONDS).toJavaDuration())
                for (record in records) {
                    log.info(
                        "CONSUMER " +
                            "Group Id:${groupId.padEnd(10)} " +
                            "Consumer Id:${consumerId.toString().padEnd(10)} " +
                            "Key:${record.key()?.padEnd(4)} " +
                            "Partition: ${record.partition()} " +
                            "Offset: ${record.offset().toString().padEnd(5)}"
                    )
                }
            }
        } catch (e: WakeupException) {
            log.info("Wake up exception!")
            // we ignore this as this is an expected exception when closing a consumer
        } catch (e: Exception) {
            log.error("Unexpected exception", e)
        } finally {
            consumer.close()
            log.info("The consumer is now gracefully closed.")
        }
    }
}
