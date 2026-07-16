module;

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

module zenoh.ke;

// Chunk-based wildcard matching (`*`, `**`) for Zenoh key expressions. See
// ke.cppm's file header for the non-`$*`/non-`@` v1 scope note.

namespace zenoh::ke {

namespace {

// Split `expr` on '/' into chunk views borrowing from `expr`. An empty `expr`
// yields one empty chunk (caller-detected as invalid), matching how a leading/
// trailing/doubled '/' also produces an empty chunk. Bails out (returning
// only the chunks found so far) the moment `max_ke_chunks + 1` chunks have
// been produced, so an adversarially long, '/'-heavy untrusted input never
// grows the returned vector past a bounded size -- the O(n) split itself,
// not just the O(n*m) matching it feeds, is bounded.
[[nodiscard]] auto split_chunks(std::string_view expr) -> std::vector<std::string_view> {
    std::vector<std::string_view> chunks;
    std::size_t start = 0;
    while (true) {
        std::size_t const slash = expr.find('/', start);
        std::size_t const end = (slash == std::string_view::npos) ? expr.size() : slash;
        chunks.push_back(expr.substr(start, end - start));
        if (chunks.size() > max_ke_chunks) {
            break; // over budget: caller checks size() and rejects
        }
        if (slash == std::string_view::npos) {
            break;
        }
        start = slash + 1;
    }
    return chunks;
}

[[nodiscard]] auto is_star(std::string_view chunk) noexcept -> bool { return chunk == "*"; }
[[nodiscard]] auto is_dstar(std::string_view chunk) noexcept -> bool { return chunk == "**"; }

// A chunk is a valid v1 wildcard chunk if it's a plain literal (no '*' at
// all) or exactly "*"/"**". Any other use of '*' (e.g. "$*", "ab*cd") is
// `$*`-glob syntax, out of v1 scope (see ke.cppm's file header).
[[nodiscard]] auto is_valid_chunk(std::string_view chunk) noexcept -> bool {
    if (chunk.empty()) {
        return false;
    }
    bool const has_star = chunk.find('*') != std::string_view::npos;
    return !has_star || is_star(chunk) || is_dstar(chunk);
}

} // namespace

auto is_canon(std::string_view expr) noexcept -> bool {
    auto const chunks = split_chunks(expr);
    if (chunks.size() > max_ke_chunks) {
        return false;
    }
    bool prev_dstar = false;
    for (auto const chunk : chunks) {
        if (!is_valid_chunk(chunk)) {
            return false;
        }
        bool const dstar = is_dstar(chunk);
        if (prev_dstar && dstar) {
            return false; // consecutive "**" is not canonical
        }
        // "**" immediately followed by "*" is not canonical: the two forms
        // denote the same language (** eating k>=0 chunks then * eating one
        // more == * eating one chunk then ** eating k>=0 more), and the
        // canonical choice is "*/**" -- this ordering requirement is what
        // makes `includes`'s DP order-invariant (see its doc comment).
        if (prev_dstar && is_star(chunk)) {
            return false;
        }
        prev_dstar = dstar;
    }
    return true;
}

auto canonize(std::string& s) noexcept -> bool {
    auto const chunks = split_chunks(s);
    if (chunks.size() > max_ke_chunks) {
        return false;
    }
    std::vector<std::string_view> out;
    out.reserve(chunks.size());
    for (auto const chunk : chunks) {
        if (!is_valid_chunk(chunk)) {
            return false;
        }
        out.push_back(chunk);
    }

    // Fixed-point normalization: collapse consecutive "**" runs, and bubble a
    // "*" left past an immediately preceding "**" (canonical form requires
    // "*/**", never "**/*" -- see is_canon's comment for why). Each swap
    // moves a "*" strictly left and each erase strictly shrinks the vector,
    // so this terminates; bounded by max_ke_chunks so the worst-case O(n^2)
    // bubble is small.
    bool changed = true;
    while (changed) {
        changed = false;
        for (std::size_t i = 0; i + 1 < out.size();) {
            if (is_dstar(out[i]) && is_dstar(out[i + 1])) {
                out.erase(out.begin() + static_cast<std::ptrdiff_t>(i) + 1);
                changed = true;
                continue; // re-examine index i against its new neighbor
            }
            if (is_dstar(out[i]) && is_star(out[i + 1])) {
                std::swap(out[i], out[i + 1]);
                changed = true;
            }
            ++i;
        }
    }

    std::string result;
    result.reserve(s.size());
    for (std::size_t i = 0; i < out.size(); ++i) {
        if (i != 0) {
            result.push_back('/');
        }
        result.append(out[i]);
    }
    s = std::move(result);
    return true;
}

// Iterative DP over two rolling rows (not the classic recursive formulation):
// dp[j] holds f(i, j) = "does A[i:] intersect B[j:]" while dp_next holds
// f(i+1, ·), for i counting down from A.size() to 0. O(n*m) time, O(m)
// memory, no recursion — the recursive form is exponential on adversarial
// alternating-"**" inputs, a real DoS surface once fed untrusted broker
// input; max_ke_chunks bounds n*m as a second line of defense.
//
// Recurrence (i < n, j < m):
//   A[i] == "**": dp[j] = dp_next[j] || dp[j+1]      -- "**" matches zero
//     chunks (drop it, f(i+1,j)) or eats one more chunk of B and stays
//     (f(i,j+1)); symmetric if B[j] == "**" instead.
//   otherwise:     dp[j] = chunk_match(A[i],B[j]) && dp_next[j+1]
//     where chunk_match is true for "*"/"*" or equal literals.
// Base rows/columns: f(n,m)=true; f(n,j)=B[j]=="**" && f(n,j+1); symmetric
// for f(i,m).
auto intersects(std::string_view a, std::string_view b) noexcept -> bool {
    auto const ca = split_chunks(a);
    auto const cb = split_chunks(b);
    std::size_t const n = ca.size();
    std::size_t const m = cb.size();
    if (n > max_ke_chunks || m > max_ke_chunks) {
        return false;
    }

    std::vector<bool> dp_next(m + 1, false);
    std::vector<bool> dp(m + 1, false);

    // Row i == n (A exhausted).
    dp_next[m] = true;
    for (std::size_t j = m; j-- > 0;) {
        dp_next[j] = is_dstar(cb[j]) && dp_next[j + 1];
    }

    for (std::size_t i = n; i-- > 0;) {
        bool const a_dstar = is_dstar(ca[i]);
        dp[m] = a_dstar && dp_next[m];
        for (std::size_t j = m; j-- > 0;) {
            if (a_dstar || is_dstar(cb[j])) {
                dp[j] = dp_next[j] || dp[j + 1];
            } else {
                bool const chunk_match = is_star(ca[i]) || is_star(cb[j]) || ca[i] == cb[j];
                dp[j] = chunk_match && dp_next[j + 1];
            }
        }
        dp_next.swap(dp);
    }
    return dp_next[0];
}

// Iterative DP, same two-rolling-rows shape as `intersects`, but with the
// asymmetric recurrence for one-directional inclusion (container includes
// contained). dp[j] holds f(i,j) = "does contained[j:]'s language lie
// entirely within container[i:]'s language".
//
// Recurrence (i < n, j < m), where A = container, B = contained:
//   A[i] == "**": dp[j] = dp_next[j] || dp[j+1]      -- same shape as
//     intersects: "**" either contributes nothing here (f(i+1,j)) or absorbs
//     everything B[j] could ever expand to and more, staying at i (f(i,j+1))
//     — valid because A[i] remains maximally permissive at the recursive call.
//   B[j] == "**" (and A[i] != "**"): dp[j] = false   -- "**" on the contained
//     side can expand to zero chunks, which neither a literal nor "*" on the
//     container side (both mandate exactly one consumed chunk) can cover.
//   A[i] == "*": dp[j] = dp_next[j+1]                -- "*" covers any single
//     literal or "*" chunk on the contained side (both bind exactly one slot).
//   A[i] literal: dp[j] = (B[j] == A[i]) && dp_next[j+1] -- only an identical
//     literal is covered; "*"/"**" on the contained side range over values a
//     fixed literal cannot.
// Base: f(n,m)=true; f(n,j)=false for j<m (container exhausted can only
// include the empty tail, and contained[j:]'s language is never exactly
// {[]} for j<m); f(i,m) = (A[i]=="**") && f(i+1,m) (container-side tail must
// itself be able to match empty).
auto includes(std::string_view container, std::string_view contained) noexcept -> bool {
    auto const ca = split_chunks(container);
    auto const cb = split_chunks(contained);
    std::size_t const n = ca.size();
    std::size_t const m = cb.size();
    if (n > max_ke_chunks || m > max_ke_chunks) {
        return false;
    }

    std::vector<bool> dp_next(m + 1, false);
    std::vector<bool> dp(m + 1, false);

    // Row i == n (container exhausted): only f(n,m) is true.
    dp_next[m] = true;
    // dp_next[0..m-1] already false-initialized.

    for (std::size_t i = n; i-- > 0;) {
        bool const a_dstar = is_dstar(ca[i]);
        dp[m] = a_dstar && dp_next[m];
        for (std::size_t j = m; j-- > 0;) {
            if (a_dstar) {
                dp[j] = dp_next[j] || dp[j + 1];
            } else if (is_dstar(cb[j])) {
                dp[j] = false;
            } else if (is_star(ca[i])) {
                dp[j] = dp_next[j + 1];
            } else {
                dp[j] = (cb[j] == ca[i]) && dp_next[j + 1];
            }
        }
        dp_next.swap(dp);
    }
    return dp_next[0];
}

} // namespace zenoh::ke
