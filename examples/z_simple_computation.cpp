#include <print>
#include <span>
import zenoh;

auto lock_doors() -> std::byte {
    std::println(">> Doors Locked");
    return std::byte{0};
}

auto unlock_doors() -> std::byte {
    std::println(">> Doors Unlocked");
    return std::byte{0};
}

auto main() -> int {
    if (auto z = zenoh::Session::open("tcp/127.0.0.1:7447")) {
        if (auto c = z->declare_computation("vehicle/door/lock")) {
            // recv() ends the loop when the link to the broker goes away.
            while (auto eval = c->recv()) {
                // An eval's argument may legally be empty -- that is "no command".
                auto arg = eval->argument();
                auto ret = arg.empty() || arg[0] == std::byte{0} ? unlock_doors() : lock_doors();
                std::ignore = eval->reply(std::span{&ret, 1});
            }
            return 0;
        }
        std::println("Unable to declare computation");
        return 1;
    }
    std::println("Please start zenohb before running this example");
    return 1;
}
