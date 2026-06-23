export module zenoh;

// The user-facing Zenoh API, layered on top of the `zenoh.proto` wire codec.
//
// Today this exposes the client `Session` (TCP transport to a router, with
// `put`/`try_put`); publisher/subscriber/get and queryable will be added here.
// The protocol message types stay under `zenoh.proto` and are intentionally not
// re-exported, so the public `zenoh` surface is the high-level API, not the codec.
export import zenoh.session;
