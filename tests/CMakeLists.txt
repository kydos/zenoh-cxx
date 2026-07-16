add_executable(zenoh-tests
    test_main.cpp
    test_util.cpp
    test_buffer.cpp
    test_varint.cpp
    test_ke.cpp
    test_codec.cpp
    test_ext.cpp
    test_put.cpp
    test_push.cpp
    test_init.cpp
    test_transport.cpp
    test_query.cpp
    test_query_api.cpp
    test_declare.cpp
    test_negative.cpp
    test_umbrella.cpp
    test_diff.cpp
    test_coverage.cpp
    test_session.cpp
    test_tcp.cpp
    test_strand.cpp
    test_subscriber.cpp
    test_broker.cpp
)
# Link the runtime (which transitively brings the proto codec) so the session/tcp
# integration tests can drive a real Session and those sources show up in coverage.
# Also link zenoh-broker for the broker-side tests (M6's full broker test suite).
# asio is linked directly too (zenoh-broker's own dependency on it is PRIVATE, so it
# doesn't propagate): test_broker.cpp's strand-marshaled Tables introspection needs
# asio::post itself, not just the asio::strand<...> return type Tables::strand()
# already exposes.
target_link_libraries(zenoh-tests PRIVATE zenoh zenoh-broker asio)
target_compile_features(zenoh-tests PRIVATE cxx_std_23)

add_test(NAME zenoh-tests COMMAND zenoh-tests)
