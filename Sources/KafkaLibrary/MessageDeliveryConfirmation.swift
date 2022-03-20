import Foundation

public struct MessageDeliveryConfirmation {
    var messageIdentifier: UUID?

    public let message: KafkaMessage?
    public let error: Error?

    init(messageIdentifier: UUID, message: KafkaMessage?) {
        self.messageIdentifier = messageIdentifier
        self.message = message
        self.error = nil
    }

    init(error: Error?) {
        self.message = nil
        self.error = error
    }
}
