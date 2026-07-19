// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "server-swift",
    dependencies: [
        // SwiftNIO — the idiomatic server-side networking stack for Swift.
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
    ],
    targets: [
        .executableTarget(
            name: "server-swift",
            dependencies: [
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
            ],
            swiftSettings: [
                .unsafeFlags(["-Ounchecked"], .when(configuration: .release)),
            ]
        ),
    ]
)
