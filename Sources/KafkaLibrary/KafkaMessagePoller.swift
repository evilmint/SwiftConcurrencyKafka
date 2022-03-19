import Foundation
import librdkafka

public typealias MessageThrowingStream = AsyncThrowingStream<KafkaMessage, Error>

final class KafkaMessagePoller {
    private enum Constants {
        static let messageCountToCommit = 1000
        static let pollTimeoutInMilliseconds = 500
    }

    private let kafkaClient: LibRdKafkaClient
    private let partitionList: TopicPartitionList
    private var isPollingActive = true

    init(kafkaClient: LibRdKafkaClient, partitionList: TopicPartitionList) {
        self.kafkaClient = kafkaClient
        self.partitionList = partitionList
    }

    func messageStream() -> MessageThrowingStream {
        AsyncThrowingStream { continuation in
            Task.detached {
                var messageCount = 0

                while self.isPollingActive {
                    let rdKafkaMessage = self.kafkaClient.consumerPoll(timeout: Constants.pollTimeoutInMilliseconds)
                    guard let kafkaMessage = KafkaMessage(rdKafkaMessagePointer: rdKafkaMessage) else {
                        continue
                    }

                    continuation.yield(kafkaMessage)

                    messageCount += 1
                    if messageCount % Constants.messageCountToCommit == 0 {
                        let commitError = self.kafkaClient.commitOffsets(using: self.partitionList)

                        if commitError.rawValue != 0 {
                            throw KafkaError(
                                code: commitError.rawValue,
                                // TODO: Wrap errors in custom type, or even return from kafka client methods
                                message: String(cString: rd_kafka_err2str(commitError))
                            )
                        }
                    }
                }
            }
        }
    }

    func terminatePolling() {
        isPollingActive = false
    }
}
