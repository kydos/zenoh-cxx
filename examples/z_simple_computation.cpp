#include <print>
#include <span>
import zenoh;

auto lock_doors() -> std::byte {
    std::println(">> Doors Locked");
    return std::byte{0};
}

auto unlock_doors() -> std::byte {
    std::println(">> Doors Unocked");
    return std::byte{0};
}

auto main() -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447"); z ) {
        auto c = z->declare_computation("vehicle/door/lock");
        while (c) {
            auto eval = c->recv();
            auto arg = eval->argument();
            std::byte ret;
            if (arg[0] == std::byte(0)) {
                ret = unlock_doors();
            } else {
                ret = lock_doors();
            }
            auto result = std::span{&ret, 1};
            std::ignore = eval->reply(result);
        }
        std::println("Unable to declare computation");
    }
    std::println("Please start zenohb before running this example");
    return 1;
}