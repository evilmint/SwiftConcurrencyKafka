import Foundation
import librdkafka

final class KafkaConfiguration {
    let handle: OpaquePointer!

    init() {
        handle = rd_kafka_conf_new()
    }

    func setConfig(key: String, value: String) throws {
        let error = LibRdKafkaErrorBuffer()
        let result = rd_kafka_conf_set(handle, key, value, error.buffer, error.capacity)

        if result != RD_KAFKA_CONF_OK {
            throw KafkaError(code: result.rawValue, message: error.string)
        }
    }
}
