import Foundation
import librdkafka

final public class KafkaMessage {
    private let rdKafkaMessagePointer: UnsafeMutablePointer<rd_kafka_message_t>!
    private var rdKafkaMessage: rd_kafka_message_t {
        rdKafkaMessagePointer.pointee
    }

    init?(rdKafkaMessagePointer: UnsafeMutablePointer<rd_kafka_message_t>!) {
        self.rdKafkaMessagePointer = rdKafkaMessagePointer

        if rdKafkaMessagePointer == nil {
            return nil
        }
    }

    public func data() -> Data {
        Data(
            bytesNoCopy: rdKafkaMessage.payload,
            count: rdKafkaMessage.len,
            deallocator: .none
        )
    }

    deinit {
        if rdKafkaMessagePointer != nil {
            rd_kafka_message_destroy(rdKafkaMessagePointer)
        }
    }
}
