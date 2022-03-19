import Foundation

final class LibRdKafkaErrorBuffer {
    let capacity = 100
    let buffer: UnsafeMutablePointer<CChar>

    var string: String {
        String(cString: buffer)
    }

    init() {
        buffer = UnsafeMutablePointer<CChar>.allocate(capacity: capacity)
    }
}
