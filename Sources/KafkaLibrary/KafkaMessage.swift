import Foundation
import librdkafka

final public class KafkaMessage {
    private var rdKafkaMessageMutablePointer: UnsafeMutablePointer<rd_kafka_message_t>!
    private var rdKafkaMessagePointer: UnsafePointer<rd_kafka_message_t>!

    private var rdKafkaMessage: rd_kafka_message_t {
        rdKafkaMessagePointer.pointee
    }

    init?(rdKafkaMessagePointer: UnsafeMutablePointer<rd_kafka_message_t>!) {
        self.rdKafkaMessagePointer = UnsafePointer(rdKafkaMessagePointer)
        self.rdKafkaMessageMutablePointer = rdKafkaMessagePointer

        if rdKafkaMessageMutablePointer == nil {
            return nil
        }
    }

    init?(rdKafkaMessagePointer: UnsafePointer<rd_kafka_message_t>!) {
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
        if rdKafkaMessageMutablePointer != nil {
            rd_kafka_message_destroy(rdKafkaMessageMutablePointer)
        }
    }
}
