#include <print>
#include <string>
import zenoh;

auto main() -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447"); z) {
        if (auto sub = z->declare_subscriber("vehicle/speed"); sub) {
            // recv() blocks until the next sample, and ends the loop when the link
            // to the broker goes away.
            while (auto sample = sub->recv()) {
                auto speed = std::string(reinterpret_cast<const char*>(sample->payload().data()),
                                         sample->payload().size());

                std::println("vehicle speed: {}", speed);
            }
            return 0;
        }
        std::println("Unable to declare subscriber");
        return 1;
    }
    std::println("Please start zenohb before running the example");
    return 1;
}
