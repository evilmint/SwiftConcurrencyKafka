import Foundation

// TODO: How will it coexist with KafkaTopic? This will be internal only
final class Topic {
    let handle: OpaquePointer!

    init(kafkaClient: LibRdKafkaClient, name: String) {
        handle = kafkaClient.createTopic(name: name)
    }
}
