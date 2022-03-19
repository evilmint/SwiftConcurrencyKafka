// swift-tools-version:5.5
import PackageDescription

let package = Package(
    name: "Kafka",
    platforms: [.macOS(.v12)],
    products: [
        .library(name: "KafkaLibrary", targets: ["KafkaLibrary"]),
    ],
    dependencies: [
    ],
    targets: [
        .executableTarget(
            name: "Kafka",
            dependencies: ["KafkaLibrary"]
        ),
        .target(name: "KafkaLibrary", dependencies: ["librdkafka"]),
        .binaryTarget(name: "librdkafka", path: "librdkafka.xcframework"),
    ]
)
