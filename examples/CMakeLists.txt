# Runtime example programs. These link the user-facing `zenoh` runtime library and
# talk to a real Zenoh router over TCP.
add_executable(z_put z_put.cpp)
target_link_libraries(z_put PRIVATE zenoh)
target_compile_features(z_put PRIVATE cxx_std_23)

add_executable(z_pub z_pub.cpp)
target_link_libraries(z_pub PRIVATE zenoh)
target_compile_features(z_pub PRIVATE cxx_std_23)

add_executable(z_put_float z_put_float.cpp)
target_link_libraries(z_put_float PRIVATE zenoh)
target_compile_features(z_put_float PRIVATE cxx_std_23)

add_executable(z_sub z_sub.cpp)
target_link_libraries(z_sub PRIVATE zenoh)
target_compile_features(z_sub PRIVATE cxx_std_23)

add_executable(z_pub_thr z_pub_thr.cpp)
target_link_libraries(z_pub_thr PRIVATE zenoh)
target_compile_features(z_pub_thr PRIVATE cxx_std_23)
