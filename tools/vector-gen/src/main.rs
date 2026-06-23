//! Emits golden wire vectors from the authoritative zenoh-rust codec
//! (`commons/zenoh-protocol` + `zenoh-codec`) as a C++ header. For each message
//! type the C++ side implements, we generate random instances via zenoh-rust's
//! `rand()` constructors, clear the fields we do not yet model (so the message
//! uses only modeled wire features), encode them, and emit the bytes. The C++
//! differential test decodes each and asserts a byte-identical re-encode.
//!
//! Note: `rand()` uses a non-seedable thread RNG, so regenerating produces fresh
//! vectors; the committed header is a fixed snapshot that CI runs deterministically.

use std::fmt::Write as _;

use zenoh_buffers::writer::HasWriter;
use zenoh_codec::{WCodec, Zenoh080};
use zenoh_protocol::network::{
    interest::InterestMode, Declare, Interest, Push, Request, Response, ResponseFinal,
};
use zenoh_protocol::transport::{
    Close, FrameHeader, InitAck, InitSyn, KeepAlive, OpenAck, OpenSyn,
};
use zenoh_protocol::zenoh::{Del, Err as ZErr, Put, Query, Reply};
use zenoh_protocol::zenoh::{PushBody, RequestBody, ResponseBody};

macro_rules! enc {
    ($msg:expr) => {{
        let mut buf: Vec<u8> = Vec::new();
        Zenoh080::new().write(&mut buf.writer(), &$msg).unwrap();
        buf
    }};
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn clean_put() -> Put {
    let mut p = Put::rand();
    p.ext_unknown.clear();
    p
}

fn clean_pushbody(b: &mut PushBody) {
    match b {
        PushBody::Put(p) => p.ext_unknown.clear(),
        PushBody::Del(d) => d.ext_unknown.clear(),
    }
}

const N: usize = 24;

fn main() {
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();

    for i in 0..N {
        // --- zenoh bodies (cleared of unknown exts) ---
        cases.push((leak(format!("rand_put_{i}")), enc!(clean_put())));
        {
            let mut d = Del::rand();
            d.ext_unknown.clear();
            cases.push((leak(format!("rand_del_{i}")), enc!(d)));
        }

        {
            let mut q = Query::rand();
            q.ext_unknown.clear();
            cases.push((leak(format!("rand_query_{i}")), enc!(q)));
        }
        {
            let mut e = ZErr::rand();
            e.ext_unknown.clear();
            cases.push((leak(format!("rand_err_{i}")), enc!(e)));
        }
        {
            let mut rep = Reply::rand();
            rep.ext_unknown.clear();
            clean_pushbody(&mut rep.payload);
            cases.push((leak(format!("rand_reply_{i}")), enc!(rep)));
        }

        // --- network messages ---
        {
            let mut m = Push::rand();
            clean_pushbody(&mut m.payload);
            cases.push((leak(format!("rand_push_{i}")), enc!(m)));
        }
        {
            let mut m = Request::rand();
            let RequestBody::Query(q) = &mut m.payload;
            q.ext_unknown.clear();
            cases.push((leak(format!("rand_request_{i}")), enc!(m)));
        }
        {
            let mut m = Response::rand();
            match &mut m.payload {
                ResponseBody::Reply(rep) => {
                    rep.ext_unknown.clear();
                    clean_pushbody(&mut rep.payload);
                }
                ResponseBody::Err(e) => e.ext_unknown.clear(),
            }
            cases.push((leak(format!("rand_response_{i}")), enc!(m)));
        }
        cases.push((leak(format!("rand_responsefinal_{i}")), enc!(ResponseFinal::rand())));
        cases.push((leak(format!("rand_declare_{i}")), enc!(Declare::rand())));

        {
            let it = Interest::rand();
            // zenoh-rust folds the "final" form into Interest(mode=Final); the C++
            // side models that as a separate InterestFinal message.
            let tag = if it.mode == InterestMode::Final {
                format!("rand_interestfinal_{i}")
            } else {
                format!("rand_interest_{i}")
            };
            cases.push((leak(tag), enc!(it)));
        }

        // --- transport messages ---
        {
            let mut m = InitSyn::rand();
            m.ext_region_name = None; // not yet modeled in C++
            cases.push((leak(format!("rand_initsyn_{i}")), enc!(m)));
        }
        {
            let mut m = InitAck::rand();
            m.ext_region_name = None;
            cases.push((leak(format!("rand_initack_{i}")), enc!(m)));
        }
        {
            let mut m = OpenSyn::rand();
            m.ext_remote_bound = None; // not yet modeled in C++
            cases.push((leak(format!("rand_opensyn_{i}")), enc!(m)));
        }
        {
            let mut m = OpenAck::rand();
            m.ext_remote_bound = None;
            cases.push((leak(format!("rand_openack_{i}")), enc!(m)));
        }
        cases.push((leak(format!("rand_close_{i}")), enc!(Close::rand())));
        cases.push((leak(format!("rand_keepalive_{i}")), enc!(KeepAlive)));
        cases.push((leak(format!("rand_frameheader_{i}")), enc!(FrameHeader::rand())));
    }

    print!("{}", render_header(&cases));
    eprintln!("generated {} vectors", cases.len());
}

fn render_header(cases: &[(&str, Vec<u8>)]) -> String {
    let mut s = String::new();
    s.push_str("#pragma once\n");
    s.push_str("// AUTO-GENERATED by tools/vector-gen from the authoritative zenoh-rust codec.\n");
    s.push_str("// Do not edit by hand; regenerate with:\n");
    s.push_str("//   cargo run --manifest-path tools/vector-gen/Cargo.toml > tests/diff_vectors.hpp\n");
    s.push_str("// (rand() is non-seedable, so regeneration produces fresh vectors.)\n");
    s.push_str("#include <cstddef>\n#include <string_view>\n\n");
    s.push_str("namespace diffvec {\n");
    s.push_str("struct Vec { std::string_view name; const unsigned char* data; std::size_t size; };\n\n");

    for (name, bytes) in cases {
        let _ = write!(s, "inline constexpr unsigned char {name}[] = {{");
        for (i, b) in bytes.iter().enumerate() {
            if i % 16 == 0 {
                s.push_str("\n    ");
            }
            let _ = write!(s, "0x{b:02x}, ");
        }
        if bytes.is_empty() {
            s.push_str("\n    0x00, "); // C++ forbids zero-length arrays; pad (size tracked separately)
        }
        s.push_str("\n};\n");
    }

    s.push_str("\ninline constexpr Vec all[] = {\n");
    for (name, bytes) in cases {
        let _ = writeln!(s, "    {{\"{name}\", {name}, {}}},", bytes.len());
    }
    s.push_str("};\n} // namespace diffvec\n");
    s
}
