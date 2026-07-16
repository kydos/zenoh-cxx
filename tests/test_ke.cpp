import zenoh.ke;

#include "ztest.hpp"

#include <string>

using namespace zenoh::ke;

// Vectors below are the non-`$*`/non-`@` subset ported verbatim from the
// reference implementation's
// ../zenoh-rust/commons/zenoh-keyexpr/src/key_expr/{tests.rs,canon.rs}
// (`$*`/`@`-verbatim forms are an explicit v1 gap, see ke.cppm).

TEST("intersects: reference vectors (non-$*/@ subset)") {
    CHECK(intersects("a", "a"));
    CHECK(intersects("a/b", "a/b"));
    CHECK(intersects("*", "abc"));
    CHECK(intersects("*", "xxx"));
    CHECK(!intersects("ab/*", "ab"));
    CHECK(intersects("a/*/c/*/e", "a/b/c/d/e"));
    CHECK(!intersects("a/*/c/*/e", "a/c/e"));
    CHECK(!intersects("a/*/c/*/e", "a/b/c/d/x/e"));
    CHECK(intersects("**", "abc"));
    CHECK(intersects("**", "a/b/c"));
    CHECK(intersects("ab/**", "ab"));
    CHECK(intersects("**/xyz", "a/b/xyz/d/e/f/xyz"));
    CHECK(intersects("a/**/c/**/e", "a/b/b/b/c/d/d/d/e"));
    CHECK(intersects("a/**/c/**/e", "a/c/e"));
    CHECK(intersects("a/**/c/*/e/*", "a/b/b/b/c/d/d/c/d/e/f"));
    CHECK(!intersects("a/**/c/*/e/*", "a/b/b/b/c/d/d/c/d/d/e/f"));
    CHECK(intersects("x/abc", "x/abc"));
    CHECK(!intersects("x/abc", "abc"));
    CHECK(intersects("x/*", "x/abc"));
    CHECK(!intersects("x/*", "abc"));
    CHECK(!intersects("*", "x/abc"));
}

TEST("intersects is symmetric on the reference vectors") {
    CHECK(intersects("a/*/c/*/e", "a/b/c/d/e") == intersects("a/b/c/d/e", "a/*/c/*/e"));
    CHECK(intersects("a/**/c/**/e", "a/c/e") == intersects("a/c/e", "a/**/c/**/e"));
    CHECK(intersects("**", "a/b/c") == intersects("a/b/c", "**"));
}

TEST("includes: reference vectors (non-$*/@ subset)") {
    CHECK(includes("a", "a"));
    CHECK(includes("a/b", "a/b"));
    CHECK(includes("*", "abc"));
    CHECK(includes("*", "xxx"));
    CHECK(!includes("ab/*", "ab"));
    CHECK(includes("a/*/c/*/e", "a/b/c/d/e"));
    CHECK(!includes("a/*/c/*/e", "a/c/e"));
    CHECK(!includes("a/*/c/*/e", "a/b/c/d/x/e"));
    CHECK(includes("**", "abc"));
    CHECK(includes("**", "a/b/c"));
    CHECK(includes("ab/**", "ab"));
    CHECK(includes("**/xyz", "a/b/xyz/d/e/f/xyz"));
    CHECK(includes("a/**/c/**/e", "a/b/b/b/c/d/d/d/e"));
    CHECK(includes("a/**/c/**/e", "a/c/e"));
    CHECK(includes("a/**/c/*/e/*", "a/b/b/b/c/d/d/c/d/e/f"));
    CHECK(!includes("a/**/c/*/e/*", "a/b/b/b/c/d/d/c/d/d/e/f"));
    CHECK(includes("x/abc", "x/abc"));
    CHECK(!includes("x/abc", "abc"));
    CHECK(includes("x/*", "x/abc"));
    CHECK(!includes("x/*", "abc"));
    CHECK(!includes("*", "x/abc"));
}

TEST("includes is not symmetric: * never includes **") {
    // A single "*" always consumes exactly one chunk; "**" can also match
    // zero chunks, which a fixed-arity "*" cannot cover.
    CHECK(!includes("*", "**"));
    CHECK(includes("**", "*"));
    CHECK(!includes("a/*", "a/**"));
    CHECK(includes("a/**", "a/*"));
}

TEST("includes: a literal never includes a wildcard, only itself") {
    CHECK(includes("a", "a"));
    CHECK(!includes("a", "*"));
    CHECK(!includes("a", "**"));
    CHECK(!includes("a", "b"));
}

TEST("is_canon accepts plain literals and well-formed wildcard chunks") {
    CHECK(is_canon("a"));
    CHECK(is_canon("a/b/c"));
    CHECK(is_canon("*"));
    CHECK(is_canon("**"));
    CHECK(is_canon("a/*/c"));
    CHECK(is_canon("a/**/c"));
    CHECK(is_canon("a/**"));
}

TEST("is_canon rejects empty expr, empty chunks, and consecutive **") {
    CHECK(!is_canon(""));
    CHECK(!is_canon("/a/b/"));     // leading + trailing '/' -> empty chunks
    CHECK(!is_canon("/a/b"));      // leading '/' -> empty first chunk
    CHECK(!is_canon("a/b/"));      // trailing '/' -> empty last chunk
    CHECK(!is_canon("a//b"));      // doubled '/' -> empty middle chunk
    CHECK(!is_canon("a/**/**/b")); // consecutive "**" is not canonical
}

TEST("is_canon rejects **/* ordering; accepts */**") {
    CHECK(!is_canon("**/*"));
    CHECK(is_canon("*/**"));
    CHECK(!is_canon("a/**/*/b"));
    CHECK(is_canon("a/*/**/b"));
}

TEST("is_canon rejects $*-glob chunks (out of v1 scope)") {
    CHECK(!is_canon("ab*cd"));
    CHECK(!is_canon("a/*b/c"));
}

TEST("canonize collapses consecutive ** runs (reference vectors)") {
    std::string s1 = "hello/**/**/bye";
    CHECK(canonize(s1));
    CHECK(s1 == "hello/**/bye");

    std::string s2 = "hello/**/**";
    CHECK(canonize(s2));
    CHECK(s2 == "hello/**");

    std::string s3 = "a/**/**/**/b/**";
    CHECK(canonize(s3));
    CHECK(s3 == "a/**/b/**");
}

TEST("canonize leaves already-canonical strings unchanged") {
    std::string s = "a/*/b/**/c";
    CHECK(canonize(s));
    CHECK(s == "a/*/b/**/c");
}

TEST("canonize reorders **/* to */** (reference vector)") {
    std::string s1 = "hello/**/*";
    CHECK(canonize(s1));
    CHECK(s1 == "hello/*/**");

    std::string s2 = "**/*";
    CHECK(canonize(s2));
    CHECK(s2 == "*/**");

    // Cascading: collapsing "**/**"" after a swap can create a fresh
    // **/* adjacency that also needs bubbling.
    std::string s3 = "a/**/*/*/b";
    CHECK(canonize(s3));
    CHECK(s3 == "a/*/*/**/b");
    CHECK(is_canon(s3));
}

TEST("includes agrees regardless of **/* vs */** input order, once canonicalized") {
    std::string non_canon = "**/*";
    std::string canon = "*/**";
    CHECK(canonize(non_canon));
    CHECK(non_canon == canon);
    CHECK(includes(canon, "**/a/b") == includes(non_canon, "**/a/b"));
}

TEST("canonize fails on empty chunks (reference error vectors)") {
    std::string s1 = "/a/b/";
    CHECK(!canonize(s1));
    std::string s2 = "/a/b";
    CHECK(!canonize(s2));
    std::string s3 = "a/b/";
    CHECK(!canonize(s3));
}

TEST("canonize output is is_canon") {
    std::string s = "a/**/**/*/**/**/b";
    CHECK(canonize(s));
    CHECK(is_canon(s));
}

TEST("exactly max_ke_chunks chunks is accepted; one more is rejected") {
    std::string at_bound;
    for (std::size_t i = 0; i < max_ke_chunks; ++i) {
        if (i != 0) {
            at_bound += '/';
        }
        at_bound += 'a';
    }
    CHECK(is_canon(at_bound));
    CHECK(intersects(at_bound, at_bound));
    CHECK(includes(at_bound, at_bound));

    std::string over_bound = at_bound + "/a";
    CHECK(!is_canon(over_bound));
    CHECK(!intersects(over_bound, over_bound));
    CHECK(!includes(over_bound, over_bound));
}
