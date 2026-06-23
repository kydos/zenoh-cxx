export module zenoh.proto;

// Umbrella for the Zenoh wire protocol: every message type plus the codec
// essentials needed to encode/decode them (the error type and the byte cursors).
// Import this to work with the protocol layer; import individual leaf modules
// (e.g. `zenoh.proto.network`) to minimize rebuild fan-out.
//
// The codec primitives (`zenoh.codec`, `zenoh.codec.ext`, `zenoh.varint`) and
// internal plumbing (`ByteField`, `ext_*`, LE helpers) are intentionally NOT
// re-exported here — they are implementation detail of the message codecs.

export import zenoh.util;   // CodecError
export import zenoh.buffer; // ByteReader / ByteWriter (+ Readable/Writable concepts)
export import zenoh.proto.fields;
export import zenoh.proto.exts;
export import zenoh.proto.network;
export import zenoh.proto.transport;
export import zenoh.proto.declare;
export import zenoh.proto.interest;
