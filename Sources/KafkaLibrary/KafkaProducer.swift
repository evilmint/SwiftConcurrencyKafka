import Foundation
import librdkafka

public final class KafkaProducer {
    private enum Constants {
        static let flushTimeout: Int = 10 * 1000 // TODO: Adjust later on
    }

    private let messageDeliveryReportSerialQueue = DispatchQueue(label: "com.kafkaproducer.queue.reporting")
    private let configuration: ConfigurationProperties
    private var kafkaClient: LibRdKafkaClient!
    private var messageDeliveryExpectations: [UUID: MessageDeliveryExpectation] = [:]
    let config = KafkaConfiguration()

    public init(configuration: ConfigurationProperties) throws {
        self.configuration = configuration

        for (key, value) in configuration {
            try config.setProperty(key: key, value: value)
        }

        config.setMessageCallbackOpaquePointer(self)
        config.enableMessageCallback()

        kafkaClient = LibRdKafkaClient(kafkaHandle: try KafkaHandle(from: config, type: .producer))
    }

    func reportMessageDelivery(_ confirmation: MessageDeliveryConfirmation) {
        messageDeliveryReportSerialQueue.sync {
            if let messageIdentifier = confirmation.messageIdentifier,
               let expectation = messageDeliveryExpectations[messageIdentifier] {
                expectation.expected = confirmation
                expectation.dispatchGroup.leave()
            }
        }
    }

    public func produce<T>(
        in topic: KafkaTopic,
        _ value: T
    ) async throws -> MessageDeliveryConfirmation where T: Encodable {
        try await produce(in: topic, JSONEncoder().encode(value))
    }

    public func produce(
        in topic: KafkaTopic,
        _ data: Data,
        key: Data? = nil
    ) async throws -> MessageDeliveryConfirmation {
        try await withCheckedThrowingContinuation { continuation in
            let topic = Topic(kafkaClient: self.kafkaClient, name: topic.name)

            // TODO: think about key and flags later on
            var dataCopy = data
            dataCopy.withUnsafeMutableBytes { unsafeMutableRawBufferPointer in
                let unsafePayloadPointer = unsafeMutableRawBufferPointer.baseAddress
                let payloadByteCount = unsafeMutableRawBufferPointer.count

                let dispatchGroup = DispatchGroup()
                dispatchGroup.enter()
                let uuid = UUID()
                let deliveryExpectation = MessageDeliveryExpectation(messageIdentifier: uuid, dispatchGroup: dispatchGroup)
                messageDeliveryExpectations[deliveryExpectation.messageIdentifier] = deliveryExpectation

                guard let payloadPointer = unsafePayloadPointer else {
                    let error = KafkaError(code: nil, message: "unsafeMutableRawPointer was not initialized.")
                    continuation.resume(with: .failure(error))
                    return
                }
                do {
                    try self.kafkaClient.produce(
                        in: topic,
                        payload: payloadPointer,
                        payloadLength: payloadByteCount,
                        messageOpaque: UnsafeMutableRawPointer(Unmanaged.passUnretained(deliveryExpectation).toOpaque())
                    )
                } catch {
                    continuation.resume(with: .failure(error))
                    return
                }

                self.kafkaClient.poll()

                Task.detached {
                    let res = dispatchGroup.wait(wallTimeout: .now() + 10)
                    let expected = self.messageDeliveryReportSerialQueue.sync {
                        self.messageDeliveryExpectations[uuid]?.expected
                    }

                    if let value = expected {
                        continuation.resume(returning: value)
                    } else {
                        continuation.resume(throwing: KafkaError(code: nil, message: "Unknown error"))
                    }
                }
            }
        }
    }

    deinit {
        kafkaClient.flush(timeout: Constants.flushTimeout)
    }
}
