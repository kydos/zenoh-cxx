add_executable(zenoh-tests
    test_main.cpp
    test_util.cpp
    test_buffer.cpp
    test_varint.cpp
    test_codec.cpp
    test_ext.cpp
    test_put.cpp
    test_push.cpp
    test_init.cpp
    test_transport.cpp
    test_query.cpp
    test_declare.cpp
    test_negative.cpp
    test_umbrella.cpp
    test_diff.cpp
    test_session.cpp
    test_tcp.cpp
    test_strand.cpp
    test_subscriber.cpp
)
# Link the runtime (which transitively brings the proto codec) so the session/tcp
# integration tests can drive a real Session and those sources show up in coverage.
target_link_libraries(zenoh-tests PRIVATE zenoh)
target_compile_features(zenoh-tests PRIVATE cxx_std_23)

add_test(NAME zenoh-tests COMMAND zenoh-tests)
