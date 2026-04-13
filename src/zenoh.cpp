module;

#include <iostream>

module zenoh;

std::string_view zenoh::version() {
    return "2.0.0-alpha";
}