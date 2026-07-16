# Vendored dependency: standalone Asio

Source: https://github.com/chriskohlhoff/asio
Pinned commit: `bbecff21a23b97c34641f0f1f08b28c91b9c77cf` (tag `asio-1-38-1`)
Vendored subtree: upstream `include/` (standalone/non-Boost Asio headers, i.e.
the tree upstream's own `asio/include` symlinks to), copied verbatim under
`third_party/asio/asio/include/` here to match the include path convention
`#include <asio/...>` / `#include <asio.hpp>` expects.

Header-only, no build artifacts vendored (no Makefile.am/.pc.in/.gitignore —
stripped after copying since only the headers are consumed by this project).

License: Boost Software License 1.0, see `LICENSE_1_0.txt` in this directory
(copied verbatim from upstream).

Only the broker (`zenoh-broker`/`zenohb`) depends on this; `zenoh-proto` and
the client `zenoh` library never do — see CLAUDE.md / the broker plan for the
rationale (vendored rather than fetched at configure time, to keep the build
hermetic/offline — this project's existing convention).

To update: re-clone the upstream repo at a new pinned tag, copy its `include/`
directory over this one, update the commit/tag above, and re-run the ASIO
module-boundary smoke check.
