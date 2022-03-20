import Foundation
import librdkafka

final class KafkaConfiguration {
    let handle: OpaquePointer!

    init() {
        handle = rd_kafka_conf_new()
    }

    func setProperty(key: String, value: String) throws {
        let error = LibRdKafkaErrorBuffer()
        let result = rd_kafka_conf_set(handle, key, value, error.buffer, error.capacity)

        if result != RD_KAFKA_CONF_OK {
            throw KafkaError(code: result.rawValue, message: error.string)
        }
    }

    func setMessageCallbackOpaquePointer(_ value: AnyObject) {
        rd_kafka_conf_set_opaque(handle, UnsafeMutableRawPointer(Unmanaged.passUnretained(value).toOpaque()))
    }

    func enableMessageCallback() {
        rd_kafka_conf_set_dr_msg_cb(handle) { _, kafkaMessage, pointer in
            guard let pointer = pointer else { return }
            guard let kafkaMessagePointee = kafkaMessage?.pointee else { return }

            let kafkaProducer = Unmanaged<KafkaProducer>.fromOpaque(pointer).takeUnretainedValue()
            let messageDeliveryExpectation = Unmanaged<MessageDeliveryExpectation>.fromOpaque(kafkaMessagePointee._private).takeUnretainedValue()

            if kafkaMessagePointee.err.rawValue != 0 {
                kafkaProducer.reportMessageDelivery(
                    MessageDeliveryConfirmation(
                        error: KafkaError(error: kafkaMessagePointee.err)
                    )
                )
            } else  {
                if let kafkaMessage = KafkaMessage(rdKafkaMessagePointer: kafkaMessage) {
                    kafkaProducer.reportMessageDelivery(
                        MessageDeliveryConfirmation(
                            messageIdentifier: messageDeliveryExpectation.messageIdentifier,
                            message: kafkaMessage
                        )
                    )
                }
            }
        }
    }
}
