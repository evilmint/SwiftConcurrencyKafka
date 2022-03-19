import Foundation

struct KafkaError: Error {
    let code: Int32?
    let message: String


    init(code: Int32?, message: String) {
        self.code = code
        self.message = message
    }
}
