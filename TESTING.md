# Testing

The suite is split into three layers:

| Layer | Location | What it covers |
|---|---|---|
| Unit (in-crate) | `rafty/src/**`, `kiwi/src/**` | private helpers, wire shapes, size budgets |
| Library integration | `rafty/tests/*.rs` | state machine, voting/quorum, push/commit, heartbeat, jobs, serialization, in-memory log/storage, cluster membership/join, simulated 3-node cluster |
| HTTP integration | `kiwi/tests/http.rs` | routing, extractor/status behavior, DI wiring, error responses |

Shared test doubles live in [`rafty/tests/support/mod.rs`](rafty/tests/support/mod.rs):

- `FakeLog` / `FakeStorage` — inspectable, with injectable failures.
- `Recorder` + `Behavior` — a peer that can approve, reject, delay, hang or reject configuration updates.
- `JoinResponder` — a seed that accepts, redirects or rejects a join request.
- `cluster_client(local, peers)` — builds a `ClusterClient` around those peers.
- `cluster_client_with_configuration(local, configuration, peers)` — same, with an explicit membership and addresses.
- `ScriptedFollower` — replays a queue of `LogReplicationResponse`s.
- Peer RPCs are dispatched directly at the `RemoteNode` boundary; no sockets are involved.

## Running

```sh
# maintained library suite
cargo test -p rafty

# one file
cargo test -p rafty --test heartbeat

# only the documented defects (see below) — these are expected to fail
cargo test -p rafty -- --ignored
```

`cargo test --workspace` additionally builds `kiwi`'s HTTP test target, which is still
scaffold code and does not compile against the current wire types.

Result for the maintained `rafty` suite: **101 passing, 0 failing, 2 ignored**.
`kiwi`'s HTTP test target is still scaffold code and is not part of that count.

## Known defects encoded as `#[ignore]` tests

Each test below asserts the *correct* behavior and currently fails. Remove the
`#[ignore]` once the underlying defect is fixed.

| Defect | Tests |
|---|---|
| `VoteResponse.voted` is ignored; a "no" counts as a yes | `election_is_lost_when_peers_reject`, `vote_quorum_ignores_no_votes` |
| Vote response echoes the proposed term, not the responder's | `vote_response_reports_the_responders_term` |
| Heartbeats never merge the term and never replace a stale leader | `heartbeat_advances_the_local_term`, `heartbeat_from_a_new_leader_replaces_the_stale_one`, `heartbeat_with_a_lower_term_is_rejected` |
| A follower with no known leader rejects its first heartbeat (no bootstrap) | `first_heartbeat_bootstraps_the_leader` |
| The heartbeat handler mutates a clone, so the leader is never stored | `first_heartbeat_bootstraps_the_leader` |
| Heartbeat/push/commit stop at the first responder instead of all peers | `heartbeat_reaches_every_peer`, `a_write_reaches_every_follower` |
| An empty peer set can never form a quorum (single-node cluster is dead) | `single_node_cluster_can_reach_quorum` |
| `broadcast_call` has no timeout and hangs when quorum is impossible | `broadcast_without_quorum_terminates` |
| A follower accepts writes/commits without a leader | `follower_write_is_rejected_or_forwarded`, `follower_commit_is_not_applied_locally` |
| Log replication is requested from the follower's own id, not the leader | `a_lagging_follower_catches_up_from_the_leader` |
| An empty follower log never starts replication (cursor is `None`) | `fresh_follower_with_an_empty_log_starts_replication` |
| `LogReplicationJob` panics for `DateTime::MIN_UTC` (`cursor - 1ns`) | `replication_from_the_minimum_cursor_does_not_panic` |
| Replication cursor is a timestamp, so equal timestamps can be skipped | `chunked_reads_do_not_lose_equal_timestamps` |
| `JobRegistry` never calls `reset`/`stop` on launched jobs | `reset_reaches_a_started_job`, `stop_reaches_a_started_job` |
| A finished job stays "launched" and can never be restarted | `a_finished_job_can_be_started_again` |
| A failed election stops the watchdog instead of retrying | `failed_election_is_retried` |
| Term arithmetic wraps `u64::MAX` back to zero | `term_does_not_wrap_around` |
| Two elections can produce two leaders | `only_one_leader_exists_after_a_second_election` |
| Every client command turns a follower into a candidate | `command_does_not_change_the_node_state` |
| `/follower/vote` returns `null` and ignores the request | `vote_endpoint_returns_a_vote_response` |
| Client commands have no idempotency key, so retries are duplicated | `a_retried_write_is_applied_once` |
| `NaN`/`Infinity` serialize to `null` and break deserialization | `non_finite_floats_are_rejected_or_round_trip` |

## Cluster membership

Bootstrap and membership have their own coverage:

- [`rafty/src/config.rs`](rafty/src/config.rs) deserializes `rafty/config.yaml` directly
  into `Config` and validates it: `humantime` durations (`150ms`, `2s`),
  `http::uri::Authority` addresses, unique addresses, wildcard advertise rejection,
  bootstrap-node presence, `election-timeout > heartbeat-interval`, and joiner configs
  that are absent from the node list. `ConfigOverrides` is covered separately; the
  `clap` flags/env mapping is exercised through `kiwi --help` and the binary smoke test.
- [`rafty/src/cluster.rs`](rafty/src/cluster.rs) applies `AddLearner`/`Promote`/`Remove`
  entries, rejects duplicate addresses, last-voter removal, stale snapshots and entry
  gaps, and journals committed entries.
- [`rafty/src/job/join.rs`](rafty/src/job/join.rs) contacts seeds, follows redirects,
  installs the leader's configuration and retries when every seed rejects.
- [`rafty/tests/cluster.rs`](rafty/tests/cluster.rs) drives a leader through admitting a
  learner, broadcasting the entry, refusing to commit without a quorum, promoting and
  removing the learner.

The HTTP transport that would expose `JoinRequest`/`ConfigurationBroadcast` endpoints is
not implemented yet; the Docker files are a reference for when it is.

## Not covered yet

- **Real multi-process/HTTP cluster tests.** There is no HTTP implementation of
  `CandidateClient`/`LeaderClient`/`FollowerClient` yet, so the cluster tests simulate the
  transport at the `RemoteNode` boundary. Process-level tests need that transport plus
  configuration wiring.
- **Cluster configuration persistence and promotion.** Membership changes are
  replicated and applied, but `ConfigurationJournal` is in-memory only, and a learner is
  promoted explicitly (`State::<Leader>::promote_member`) rather than automatically after
  catch-up.
- **Fuzzing.** `arbitrary_json_never_panics` is a corpus test; a `cargo-fuzz` target for
  `ClusterMessage<T>` deserialization would add real coverage.
