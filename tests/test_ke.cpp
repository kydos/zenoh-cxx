import zenoh.ke;

#include "ztest.hpp"

#include <string>

using namespace zenoh::ke;

// Vectors below are the non-`$*` subset ported verbatim from the reference
// implementation's
// ../zenoh-rust/commons/zenoh-keyexpr/src/key_expr/{tests.rs,canon.rs}
// (`$*` sub-chunk globbing is an explicit v1 gap, see ke.cppm; `@`-verbatim
// chunks are implemented and have their own cases at the end of this file).

TEST("intersects: reference vectors (non-$* subset)") {
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

TEST("includes: reference vectors (non-$* subset)") {
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

// --- Verbatim ('@'-leading) chunks ------------------------------------------------
//
// A verbatim chunk names a Zenoh-reserved namespace -- `@/<zid>/...` (admin), the
// reference's `@adv`, this project's `@eval` (docs/RUNTIME.md) -- and is matched only
// by an identical literal chunk, never by a wildcard. Vectors ported from the same
// reference file as the ones above, minus its `$*` forms.

TEST("intersects: verbatim chunks are matched only by themselves (reference vectors)") {
    CHECK(intersects("@a", "@a"));
    CHECK(!intersects("@a", "@ab"));
    CHECK(!intersects("@a", "@a/b"));
    CHECK(!intersects("@a", "@a/*"));
    CHECK(!intersects("@a", "@a/*/**"));
    CHECK(intersects("@a", "@a/**"));
    CHECK(intersects("@a/**/c/**/e", "@a/b/b/b/c/d/d/d/e"));
    CHECK(!intersects("@a/**/c/**/e", "@a/@b/b/b/c/d/d/d/e"));
    CHECK(intersects("@a/**/@c/**/e", "@a/b/b/b/@c/d/d/d/e"));
    CHECK(intersects("@a/**/e", "@a/b/b/d/d/d/e"));
    CHECK(intersects("@a/**/e", "@a/b/b/b/d/d/d/e"));
    CHECK(intersects("@a/**/e", "@a/b/b/c/d/d/d/e"));
    CHECK(!intersects("@a/**/e", "@a/b/b/@c/b/d/d/d/e"));
    CHECK(!intersects("@a/*", "@a/@b"));
    CHECK(!intersects("@a/**", "@a/@b"));
    CHECK(intersects("@a/**/@b", "@a/@b"));
    CHECK(intersects("@a/@b/**", "@a/@b"));
    CHECK(intersects("@a/**/@c/**/@b", "@a/**/@c/@b"));
    CHECK(intersects("@a/**/@c/**/@b", "@a/@c/**/@b"));
    CHECK(intersects("@a/**/@c/@b", "@a/@c/**/@b"));
    CHECK(!intersects("@a/**/@b", "@a/**/@c/**/@b"));
    CHECK(intersects("@a", "**/@a"));
}

TEST("intersects: a wildcard alone never reaches a reserved namespace") {
    // The property the Evaluation abstraction's isolation rests on (session.cpp's
    // `eval_prefix`): an application key expression, however wild, cannot name a key
    // in the `@eval` namespace -- while an evaluation, which prefixes both sides
    // identically, matches exactly as it would have unprefixed.
    CHECK(!intersects("**", "@eval/robot/r1/reset"));
    CHECK(!intersects("*/robot/r1/reset", "@eval/robot/r1/reset"));
    CHECK(!intersects("**/reset", "@eval/robot/r1/reset"));
    CHECK(intersects("@eval/robot/*/reset", "@eval/robot/r1/reset"));
    CHECK(intersects("@eval/**", "@eval/robot/r1/reset"));
    CHECK(intersects("robot/*/reset", "robot/r1/reset")); // ... same match, unprefixed
    CHECK(!intersects("@eval/robot/*/reset", "@eval/robot/r1/stop"));
    // The admin space and the reference's `@adv` are equally out of reach, and
    // distinct from `@eval`.
    CHECK(!intersects("**", "@/router/gossip"));
    CHECK(!intersects("@eval/**", "@adv/pub/x"));
}

TEST("includes: verbatim chunks are covered only by themselves (reference vectors)") {
    CHECK(includes("@a", "@a"));
    CHECK(!includes("@a", "@ab"));
    CHECK(!includes("@a", "@a/b"));
    CHECK(!includes("@a", "@a/*"));
    CHECK(!includes("@a", "@a/*/**"));
    CHECK(!includes("@a", "@a/**"));
    CHECK(includes("@a/**", "@a"));
    CHECK(includes("@a/**/c/**/e", "@a/b/b/b/c/d/d/d/e"));
    CHECK(!includes("@a/*", "@a/@b"));
    CHECK(!includes("@a/**", "@a/@b"));
    CHECK(includes("@a/**/@b", "@a/@b"));
    CHECK(includes("@a/@b/**", "@a/@b"));
    CHECK(!includes("**", "@eval/robot/r1/reset"));
}

TEST("is_canon and canonize treat a verbatim chunk as the plain literal it is") {
    CHECK(is_canon("@eval/robot/r1/reset"));
    CHECK(is_canon("@eval/robot/*/reset"));
    CHECK(!is_canon("@eval//robot"));
    std::string s = "@eval/a/**/**/b";
    CHECK(canonize(s));
    CHECK(s == "@eval/a/**/b");
}
