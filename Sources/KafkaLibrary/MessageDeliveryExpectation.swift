import Foundation

final class MessageDeliveryExpectation {
    let messageIdentifier: UUID
    let dispatchGroup: DispatchGroup
    var expected: MessageDeliveryConfirmation?

    init(messageIdentifier: UUID, dispatchGroup: DispatchGroup) {
        self.messageIdentifier = messageIdentifier
        self.dispatchGroup = dispatchGroup
    }
}
