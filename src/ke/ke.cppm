module;

#include <cstddef>
#include <string>
#include <string_view>

export module zenoh.ke;

// Zenoh key-expression matching: canonical-form validation and wildcard
// intersection/inclusion over the `*` (single chunk) and `**` (zero-or-more
// chunks) forms, plus `@`-verbatim chunks (a chunk beginning with '@' is only
// ever matched by an identical literal, never by a wildcard -- the reference's
// `MayHaveVerbatim` rule, which is what keeps Zenoh's reserved namespaces
// `@/<zid>/...` (admin), `@adv` and this project's `@eval` (docs/RUNTIME.md)
// out of reach of an application's `**`). `$*` sub-chunk globbing (the rest of
// the full spec's wildcard vocabulary) remains an explicit, documented v1 gap:
// neither this project's own `WireExpr` usage nor its interop targets
// (docs/BROKER.md) produce it, and a chunk using it is treated as
// non-canonical rather than silently mishandled.
export namespace zenoh::ke {

/// Chunk-count bound applied to every function below: protects the DP-based
/// matching (and canonicalization) from pathological, adversarially long
/// untrusted key expressions fed to a broker. Mirrors this project's existing
/// untrusted-input-bounding idiom (e.g. `session.cpp`'s `max_key_len`).
inline constexpr std::size_t max_ke_chunks = 512;

/// Whether `expr` is already in canonical form: non-empty, no empty/doubled
/// '/'-separated chunks, every wildcard chunk is exactly "*" or "**", no two
/// consecutive "**" chunks, no "**" chunk immediately followed by a "*"
/// chunk (canonical form is always `*/**`, never `**/*` — the two orders
/// denote the same language, but `includes` below requires the normalized
/// one to stay order-invariant; see its doc comment), and at most
/// `max_ke_chunks` chunks. Pure, non-allocating.
[[nodiscard]] auto is_canon(std::string_view expr) noexcept -> bool;

/// Canonicalize `s` in place: collapses runs of consecutive "**" chunks into
/// one (e.g. `"a/**/**/b"` -> `"a/**/b"`), and reorders any `**/*` adjacency
/// to `*/**` (e.g. `"a/**/*/b"` -> `"a/*/**/b"` — same language, canonical
/// order; see `is_canon`). Returns false (leaving `s` unmodified) if `s`
/// contains a malformed chunk (empty, non-canonical wildcard use, or more
/// than `max_ke_chunks` chunks) that canonicalization cannot fix.
[[nodiscard]] auto canonize(std::string& s) noexcept -> bool;

/// True if there exists at least one concrete (wildcard-free) key matched by
/// both `a` and `b`. A chunk beginning with '@' is *verbatim*: no `*`/`**` on
/// the other side matches it, so `**` does not intersect `@eval/x` and only an
/// identical `@eval` chunk does. Both must already be canonical (callers canonicalize
/// once at declare time — e.g. the broker's resource table — not per
/// incoming message), though `intersects` itself is verified order-invariant
/// (branches on chunk content at each position rather than assuming a
/// normalized order) so a non-canonical input never produces a wrong answer,
/// only a possibly-redundant one. Returns false if either side exceeds
/// `max_ke_chunks` chunks.
[[nodiscard]] auto intersects(std::string_view a, std::string_view b) noexcept -> bool;

/// True if every concrete key matched by `contained` is also matched by
/// `container` (i.e. `container`'s pattern is a superset of `contained`'s).
/// Verbatim ('@'-leading) chunks obey the same rule as in `intersects`: a
/// `*`/`**` on the container side never covers one on the contained side.
/// Both **must** already be canonical (per `is_canon`, including the `**/*`
/// -> `*/**` ordering): unlike `intersects`, this DP's correctness genuinely
/// depends on that normalized order — a `**` immediately followed by a `*`
/// on the container side is handled differently than a `*` followed by
/// `**`, even though the two describe the same language, so an
/// un-canonicalized container/contained pair can produce a wrong answer, not
/// just a redundant one. Not required for v1 routing correctness (only
/// `intersects` gates broker fan-out); provided for future route-cache
/// subsumption optimizations. Returns false if either side exceeds
/// `max_ke_chunks` chunks.
[[nodiscard]] auto includes(std::string_view container, std::string_view contained) noexcept
    -> bool;

} // namespace zenoh::ke
