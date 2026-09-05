#include <chrono>
#include <print>
#include <thread>
import zenoh;

auto main(int argc, char *argv[]) -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447"); z) {
        auto sub = z->declare_subscriber("vehicle/speed");
        while (true) {
            if (auto sample = sub->recv(); sample) {
                auto speed = std::string(reinterpret_cast<const char*>(sample->payload().data()), sample->payload().size());

                std::println("vehicle speed: {}", speed);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
    std::println("Please start zenohb before running the example");
    return 1;

}