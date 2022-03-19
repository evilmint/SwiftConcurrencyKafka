import Foundation
import librdkafka

public final class KafkaConsumer {
    private let configuration: ConfigurationProperties
    private let kafkaClient: LibRdKafkaClient

    public init(configuration: ConfigurationProperties) throws {
        self.configuration = configuration

        let config = KafkaConfiguration()

        for (key, value) in configuration {
            try config.setConfig(key: key, value: value)
        }

        kafkaClient = LibRdKafkaClient(kafkaHandle: try KafkaHandle(from: config, type: .consumer))
    }

    public func subscribe(to topics: [KafkaTopic]) throws -> MessageThrowingStream {
        kafkaClient.redirectMainQueueToConsumer()
        let partitionList = TopicPartitionList(kafkaClient: kafkaClient, topics: topics)
        try partitionList.subscribe()
        let messagePoller = KafkaMessagePoller(kafkaClient: kafkaClient, partitionList: partitionList)
        return messagePoller.messageStream()
    }
}
