import Foundation

struct MessageConfirmation {
}

public final class KafkaProducer {
    private enum Constants {
        static let flushTimeout: Int = 10 * 1000 // TODO: Adjust later on
    }

    private let configuration: ConfigurationProperties
    private let kafkaClient: LibRdKafkaClient

    private var messageConfirmationContinuation: AsyncStream<MessageConfirmation>.Continuation?
    private var messageConfirmationQueue: [MessageConfirmation] = []

    var messageConfirmations: AsyncStream<MessageConfirmation> {
        let stream = AsyncStream<MessageConfirmation> { continuation in
            messageConfirmationContinuation = continuation

            Task.detached {
                while !Task.isCancelled {
                    try await Task.sleep(nanoseconds: 10_000_000)
                }
            }
        }

        return stream
    }

    public init(configuration: ConfigurationProperties) throws {
        self.configuration = configuration

        let config = KafkaConfiguration()

        for (key, value) in configuration {
            try config.setConfig(key: key, value: value)
        }

        kafkaClient = LibRdKafkaClient(kafkaHandle: try KafkaHandle(from: config, type: .producer))

//        rd_kafka_conf_set_dr_msg_cb(config.handle) { [unowned self] ptr, kafkaMessage, pointer in
//            pushToCallback?()
//            // self?.messageConfirmationQueue.append(MessageConfirmation())
//            print("Callback called")
//            if let err = kafkaMessage?.pointee.err, err.rawValue != 0 {
//                print(String(cString: rd_kafka_err2str(err)))
//            } else if let kafkaMessage = kafkaMessage {
//                let deliverMessage = String(format: "Delivered %d bytes to partition %d", kafkaMessage.pointee.len, kafkaMessage.pointee.partition)
//                print(deliverMessage)
//            }
//        }
    }

    public func produce<T>(in topic: KafkaTopic, _ value: T) async throws where T: Encodable {
        try await produce(in: topic, JSONEncoder().encode(value))
    }

    public func produce(in topic: KafkaTopic, _ data: Data, key: Data? = nil) async throws {
        try await withCheckedThrowingContinuation { (continuation: (CheckedContinuation<Void, Error>)) in
            let topic = Topic(kafkaClient: self.kafkaClient, name: topic.name)

            // TODO: think about key and flags later on
            // TODO: msg_opaque as kind of "info" passed in callback

            var dataCopy = data
            var unsafePayloadPointer: UnsafeMutableRawPointer?
            var payloadByteCount: Int?
            dataCopy.withUnsafeMutableBytes { unsafeMutableRawBufferPointer in
                unsafePayloadPointer = unsafeMutableRawBufferPointer.baseAddress
                payloadByteCount = unsafeMutableRawBufferPointer.count

                guard let payloadPointer = unsafePayloadPointer,
                      let byteCountUnwrapped = payloadByteCount else {
                    let error = KafkaError(code: nil, message: "unsafeMutableRawPointer was not initialized.")
                    continuation.resume(with: .failure(error))
                    return
                }

                do {
                    try kafkaClient.produce(
                        in: topic,
                        payload: payloadPointer,
                        payloadLength: byteCountUnwrapped
                    )
                } catch {
                    continuation.resume(with: .failure(error))
                }

                kafkaClient.poll()
                continuation.resume()
            }
        }
    }

    deinit {
        kafkaClient.flush(timeout: Constants.flushTimeout)
    }
}
