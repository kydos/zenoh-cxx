#include <chrono>
#include <print>
#include <span>
#include <string>
#include <thread>

import zenoh;

auto read_speed() -> std::uint16_t { return static_cast<unsigned short>(std::rand() % 200); }

auto main() -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447")) {
        if (auto pub = z->declare_publisher("vehicle/speed")) {
            for (;;) {
                auto speed = read_speed();
                auto str = std::to_string(speed);
                std::println("speed: {}", str);
                // put() fails once the link to the broker goes away.
                if (!pub->put(std::as_bytes(std::span{str}))) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            return 0;
        }
        std::println("Unable to declare publisher");
        return 1;
    }
    std::println("Please start zenohb before running the example");
    return 1;
}
