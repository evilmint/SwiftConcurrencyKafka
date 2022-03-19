import Foundation
import librdkafka

// TODO: Merge with KafkaClient?
final class KafkaHandle {
    enum HandleType {
        case producer
        case consumer
    }

    let handle: OpaquePointer!

    init(from config: KafkaConfiguration, type: HandleType) throws {
        let initError = LibRdKafkaErrorBuffer()
        let kafkaType: rd_kafka_type_t

        switch type {
        case .consumer:
            kafkaType = RD_KAFKA_CONSUMER
        case .producer:
            kafkaType = RD_KAFKA_PRODUCER
        }

        handle = rd_kafka_new(kafkaType, config.handle, initError.buffer, initError.capacity)
    
        if handle == nil {
            throw KafkaError(code: nil, message: initError.string)
        }
    }

    deinit {
        rd_kafka_consumer_close(handle)
        rd_kafka_destroy(handle)
    }
}
