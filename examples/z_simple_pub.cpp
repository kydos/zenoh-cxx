#include <chrono>
#include <print>
#include <span>
#include <string>
#include <thread>

import zenoh;

auto read_speed() -> std::uint16_t {
    return static_cast<unsigned short>(std::rand() % 200);
}

auto main(int argc, char** argv) -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447"); z) {
        auto pub = z->declare_publisher("vehicle/speed");
        while (true) {
            auto speed = read_speed();
            auto str = std::to_string(speed);
            std::println("speed: {}", str);
            std::ignore = pub->put(std::as_bytes(std::span{str}));
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
    std::println("Please start zenohb before running the example");
    return 1;
}
