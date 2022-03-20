import Foundation
import librdkafka

struct KafkaError: Error {
    let code: Int32?
    let message: String

    init(code: Int32?, message: String) {
        self.code = code
        self.message = message
    }

    init(error: rd_kafka_resp_err_t) {
        code = error.rawValue
        message = String(cString: rd_kafka_err2str(error))
    }
}
