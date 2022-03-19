import Foundation
import librdkafka

final class TopicPartitionList {
    private let kafkaClient: LibRdKafkaClient
    let partitionList: LibRdKafkaPartitonList

    init(kafkaClient: LibRdKafkaClient, topics: [KafkaTopic]) {
        self.kafkaClient = kafkaClient
        partitionList = kafkaClient.createTopicPartitionList(topicCount: topics.count)

        for topic in topics {
            // TODO: Extract RD_KAFKA_PARTITION_UA as own type?
            kafkaClient.addTopicToPartitionList(topic, partitionList: partitionList, partition: RD_KAFKA_PARTITION_UA)
        }
    }

    func subscribe() throws {
        let subscribeError = kafkaClient.subscribeToTopics(partitionList: partitionList)

        if subscribeError.rawValue != 0 {
            let error = KafkaError(
                code: subscribeError.rawValue,
                message: String(cString: rd_kafka_err2str(subscribeError))
            )
            throw error
        }
    }

    deinit {
        kafkaClient.destroyTopicPartitionList(partitionList)
    }
}
