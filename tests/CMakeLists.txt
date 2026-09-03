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
    test_sweep.cpp
    test_umbrella.cpp
    test_diff.cpp
    test_coverage.cpp
    test_session.cpp
    test_tcp.cpp
    test_strand.cpp
    test_subscriber.cpp
    test_broker.cpp
    test_broker_stress.cpp
    test_clique.cpp
    test_membership.cpp
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

# One ctest case per ztest TEST(...), discovered from the linked binary (see
# ZTestDiscover.cmake) so `ctest` reports the suite test-by-test, `-R`/`-L` can narrow
# it to one case or one source file, and `-j` runs cases in parallel. Set to OFF to go
# back to a single ctest case that runs the whole binary in one process.
option(ZENOH_CTEST_PER_TEST "Register one ctest case per ztest TEST(...)" ON)

if(ZENOH_CTEST_PER_TEST)
    set(ZTEST_CASES "${CMAKE_CURRENT_BINARY_DIR}/ztest_cases.cmake")
    set(ZTEST_INCLUDE "${CMAKE_CURRENT_BINARY_DIR}/ztest_include.cmake")
    # ctest reads its include file when the suite is *run*, which may be before the
    # first build has produced any case list -- so hand ctest this stable shim rather
    # than the discovered file itself. Keeping the discovered file purely a build
    # output also keeps its timestamp meaningful: a configure-time placeholder would
    # look newer than the binary and suppress the discovery step below.
    file(WRITE "${ZTEST_INCLUDE}"
         "if(EXISTS \"${ZTEST_CASES}\")\n"
         "    include(\"${ZTEST_CASES}\")\n"
         "else()\n"
         "    add_test(\"zenoh-tests-not-discovered\" \"${CMAKE_COMMAND}\" -E false)\n"
         "endif()\n")
    add_custom_command(
        OUTPUT "${ZTEST_CASES}"
        COMMAND "${CMAKE_COMMAND}" -D "TEST_EXECUTABLE=$<TARGET_FILE:zenoh-tests>"
                -D "OUTPUT_FILE=${ZTEST_CASES}" -P
                "${CMAKE_CURRENT_SOURCE_DIR}/ZTestDiscover.cmake"
        DEPENDS zenoh-tests "${CMAKE_CURRENT_SOURCE_DIR}/ZTestDiscover.cmake"
        VERBATIM
        COMMENT "Discovering ztest cases")
    add_custom_target(zenoh-tests-discover ALL DEPENDS "${ZTEST_CASES}")
    set_property(DIRECTORY APPEND PROPERTY TEST_INCLUDE_FILES "${ZTEST_INCLUDE}")
else()
    add_test(NAME zenoh-tests COMMAND zenoh-tests)
endif()
