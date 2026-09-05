#include <print>
#include <span>
import zenoh;

auto main(int argc, char** argv) -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447"); z) {
        auto e = z->declare_evaluator("vehicle/door/lock");
        auto lock = std::byte{0};
        if (argc > 1) {
            lock = std::byte{1};
        }
        auto arg = std::span{&lock, 1};
        std::ignore = e->eval(arg);
        return 0;
    }
    std::println("Please start zenohb before running this example");
    return 1;
}