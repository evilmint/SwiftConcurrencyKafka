import Foundation
import KafkaLibrary

struct Cat: Encodable {
    let name: String
}

do {
    let config = [
        "socket.timeout.ms": String(2000),
        "bootstrap.servers": "127.0.0.1:9092",
        "group.id": "test",
        "auto.offset.reset": "earliest",
    ]
//
//    let client = try KafkaConsumer(configuration: config)
//
//    Task.detached {
//        do {
//            let subscription = try client.subscribe(to: [KafkaTopic(name: "test")])
//
//            for try await message in subscription {
//                let string = String(data: message.data(), encoding: .ascii)!
//                print(string)
//            }
//        } catch {
//            print("Caught error \(error)")
//        }
//    }

    let client = try KafkaProducer(configuration: config)
    let testTopic = KafkaTopic(name: "test")

    Task.detached {
        for i in 0 ..< 10 {
            do {
                let geoffreyCat = Cat(name: "Geoffrey \(i)")
                let result = try await client.produce(in: testTopic, geoffreyCat)
                print(result.message)
            } catch {
                print("Caught error \(error)")
            }
            do {
                try await Task.sleep(nanoseconds: 2_000_000_000)
            } catch {
                print("Sleeping failed \(error)")
            }
        }
    }

    RunLoop.current.run()
} catch {
    print(error)
}

