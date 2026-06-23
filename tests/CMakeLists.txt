add_executable(zenoh-tests
    test_main.cpp
    test_util.cpp
    test_buffer.cpp
    test_varint.cpp
    test_put.cpp
    test_push.cpp
    test_init.cpp
    test_transport.cpp
    test_query.cpp
    test_declare.cpp
    test_negative.cpp
    test_umbrella.cpp
    test_diff.cpp
)
target_link_libraries(zenoh-tests PRIVATE zenoh-proto)
target_compile_features(zenoh-tests PRIVATE cxx_std_23)

add_test(NAME zenoh-tests COMMAND zenoh-tests)
