// swift-tools-version:5.4
import PackageDescription

let package = Package(
    name: "Kafka",
    platforms: [.macOS(.v10_15)],
    products: [
        .library(name: "KafkaLibrary", targets: ["KafkaLibrary"]),
    ],
    dependencies: [
    ],
    targets: [
        .executableTarget(
            name: "KafkaExample",
            dependencies: ["KafkaLibrary"]
        ),
        .target(name: "KafkaLibrary", dependencies: ["librdkafka"]),
        .binaryTarget(name: "librdkafka", path: "librdkafka.xcframework"),
    ]
)
