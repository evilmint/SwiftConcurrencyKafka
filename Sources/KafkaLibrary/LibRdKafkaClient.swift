import Foundation
import librdkafka

final class LibRdKafkaClient {
    private let kafkaHandle: KafkaHandle

    init(kafkaHandle: KafkaHandle) {
        self.kafkaHandle = kafkaHandle
    }

    func poll() {
        rd_kafka_poll(kafkaHandle.handle, 1000)
    }

    func flush(timeout: Int) {
        rd_kafka_flush(kafkaHandle.handle, Int32(timeout))
    }

    func produce(
        in topic: Topic,
        payload: UnsafeMutableRawPointer,
        payloadLength: Int,
        messageOpaque: UnsafeMutableRawPointer? = nil
    ) throws {
        let returnCode = rd_kafka_produce(
            topic.handle,
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            payload,
            payloadLength,
            nil,
            0,
            messageOpaque
        )

        if returnCode != 0 {
            throw KafkaError(
                code: returnCode,
                message: "Error while producing message [errCode=\(returnCode)]"
            )
        }
    }

    func redirectMainQueueToConsumer() {
        rd_kafka_poll_set_consumer(kafkaHandle.handle)
    }

    func createTopicPartitionList(topicCount: Int) -> LibRdKafkaPartitonList! {
        rd_kafka_topic_partition_list_new(Int32(topicCount))
    }

    func destroyTopicPartitionList(_ partitionList: LibRdKafkaPartitonList!) {
        rd_kafka_topic_partition_list_destroy(partitionList)
    }

    func addTopicToPartitionList(_ topic: KafkaTopic, partitionList: LibRdKafkaPartitonList!, partition: Int32) {
        rd_kafka_topic_partition_list_add(partitionList, topic.name, partition)
    }

    func subscribeToTopics(partitionList: LibRdKafkaPartitonList!) -> rd_kafka_resp_err_t {
        rd_kafka_subscribe(kafkaHandle.handle, partitionList)
    }

    func consumerPoll(timeout: Int) -> LibRdKafkaMessage! {
        rd_kafka_consumer_poll(kafkaHandle.handle, Int32(timeout))
    }

    func commitOffsets(using partitionList: TopicPartitionList, asynchronously: Bool = true) -> LibRdKafkaError {
        rd_kafka_commit(kafkaHandle.handle, partitionList.partitionList, asynchronously ? 1 : 0)
    }

    func createTopic(name: String, configuration: OpaquePointer! = nil) -> OpaquePointer! {
        rd_kafka_topic_new(kafkaHandle.handle, name.cString(using: .ascii), configuration)
    }
}
