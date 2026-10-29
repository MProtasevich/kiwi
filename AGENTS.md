# Workspace instructions

## Build artifacts — do not read

- `target/` (at the repo root and any nested one) holds Cargo build output. Never read, glob, grep, or open files under it.
- Scope searches to source instead of scanning the repo root: `rafty/src`, `kiwi/src`, or explicit files. Prefer `glob`/`grep` `path` arguments or patterns like `rafty/**/*.rs`.
- Never stage or commit anything from `target/`; it is already in `.gitignore`.

## Layout

- Cargo workspace with two members: `rafty/` (the library: node state, log, storage, transport, jobs) and `kiwi/` (the `kiwi` binary and its axum handlers/models).
- Shared dependency versions live in the root `Cargo.toml` under `[workspace.dependencies]`; add new dependencies there rather than pinning versions per crate.

## Project structure

Kiwi is a KV store built on the Raft consensus algorithm. The workspace splits into a protocol library and an HTTP binary:

- **`rafty/`** — the Raft library. It is request-unaware: it models the node's consensus state and the background work that drives it, and knows nothing about HTTP.
  - `node_state` — the `Node` enum and `State<T: StateOption>`, a typed-state model of `Follower`, `Candidate`, and `Leader`. Each state type exposes only the operations valid in that state; the `Node` union is what client code narrows with `try_follower` / `try_candidate` / `try_leader` before acting. A `Node` instance is the main handle used while serving requests.
  - `job` — `JobRegistry` and the request-unaware periodic jobs: election timeout, leader heartbeats, and follower log replication.
  - `remote_node` / `transport` — peer RPC abstractions (`ClusterClient` and per-role client traits) and the wire message types exchanged between nodes.
  - `log` / `storage` — traits separating the replicated command log from the materialized key-value state.
  - `payload` — value and command type definitions.
- **`kiwi/`** — the `kiwi` binary. Hosts an axum server exposing the follower-facing endpoints (heartbeat, command, vote), plus the DI wiring and HTTP-facing models. Handlers hold the shared `Node`, resolve its current state, and delegate protocol work to `rafty`.

Boundary rule: consensus and state-transition logic belongs in `rafty`; HTTP, DI, and transport wiring belongs in `kiwi`.

Status: early/WIP. The `CommandLog` and `Storage` implementations and several HTTP endpoints are still stubs, and `rafty/config.yaml` sketches the cluster configuration but is not loaded yet.

## Commands

- `cargo check --workspace` — fastest structural check after cross-crate changes.
- `cargo test -p rafty` — run the library tests.
- `cargo run -p kiwi` — start the HTTP node on `127.0.0.1:8000` (reads `.env` and `RUST_LOG` if present).
