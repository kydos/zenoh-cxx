#include <print>
#include <span>
import zenoh;

auto main(int argc, char** argv) -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447")) {
        if (auto e = z->declare_evaluator("vehicle/door/lock")) {
            // Any argument on the command line means "lock", none means "unlock".
            auto lock = argc > 1 ? std::byte{1} : std::byte{0};
            std::ignore = e->eval(std::span{&lock, 1});
            return 0;
        }
        std::println("Unable to declare evaluator");
        return 1;
    }
    std::println("Please start zenohb before running this example");
    return 1;
}
