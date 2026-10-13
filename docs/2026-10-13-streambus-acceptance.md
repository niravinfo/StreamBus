# StreamBus v1 Acceptance

Date: 2026-10-13. Broker: localhost:5552 (single node; version unconfirmed this
session — management API reachable but 401 with default credentials, and
`rabbitmqctl` is not on PATH; design §1 records 4.2.6 single node
`rabbit@local`, not re-verified here — do not treat as confirmed).
Branch: feature/phase10-acceptance. Base: 781a996
(`test: add v1 acceptance gap probes`) + uncommitted Task 3 changes (this doc +
one unit mirror test in `tests/StreamBus.UnitTests/AcceptanceTests.cs`).

Matrix discipline (binding): a row is ✅ ONLY if a test asserts the
acceptance criterion. An incidentally-exercised path with no assertion is not
a pin. Final green this session (Release): unit 179/179 passed, 0 skipped;
integration 54 passed, 2 skipped (the 2 known env-skips), 0 failed — see the
v1 verdict for the exact lines.

## Functional matrix

| ID | Criterion (one line) | Pinning tests | Status |
|---|---|---|---|
| FR-1.1 | Publish round-trips with ordered offsets 0,1,2 | `Plain_round_trip_delivers_ordered_offsets` (integration: received bytes == [0,1,2]) | ✅ |
| FR-1.2 | Missing stream surfaces `StreamDoesNotExist`, never swallowed | `Missing_stream_throws_not_found` (unit: code → `StreamBusStreamNotFoundException`) + `Missing_stream_fails_and_stays_absent` (integration) | ✅ |
| FR-1.3 | Publish never auto-creates the stream | `Missing_stream_fails_and_stays_absent` (asserts stream still absent after failed publish) | ✅ |
| FR-1.4 | Failed confirms raise to the caller | `Failed_confirm_raises_with_stream_and_status` (unit) | ✅ |
| FR-1.5 | One producer instance serves N publishes to a stream | `N_publishes_reuse_one_producer` (unit: 5 publishes, 1 `PlainCreations`) | ✅ |
| FR-2.1 | Typed `IConsumer<T>` via DI with explicit stream binding (single + multi-bus) | `Typed_round_trip_plain` + `Binding_is_explicit` (unbound stream gets nothing) + `Multi_bus_isolation` (BusA/BusB markers, per-stream delivery) + unit `Store_rejects_duplicates_but_allows_variants`, `Explicit_service_lifetime_wins` | ✅ |
| FR-2.2 | Delegate `ConsumeAsync` overload works, not deprecated | All 8 `ConsumerIntegrationTests` use the delegate handler (split, failover, broadcast, resume, branches, set/get, redelivery, inflight) | ✅ |
| FR-2.3 | Shared `Reference` = one SAC group splitting partitions, exactly once | `Super_stream_splits_across_instances_exactly_once_ordered` (24/24 distinct, per-key ordered; actual scale 4 partitions × 3 instances, not the 6×4 in the criterion text — property fully pinned, scale differs) | ✅ |
| FR-2.4 | Distinct references = broadcast, every message to each | `Distinct_references_each_receive_everything` + `Two_consumer_types_each_receive_everything` | ✅ |
| FR-2.5 | Resume from broker-stored offset on attach (create/promote/reconnect); never 0, never local | `Recreate_resumes_from_broker_stored_offset` (fresh-guard recreate: [0..4] then [5..9], no overlap) + `Failover_preserves_offsets_and_order` (promotion: 24/24, no dupes) + `Strict_retry_window_survives_kill_without_skip` (kill mid-retry → second drain [1..4]) | ✅ |
| FR-2.6 | Strict per-key ordering holds across SAC promotion | `Failover_preserves_offsets_and_order` (per-key 1..12 ordered) + `Retry_preserves_order_under_load_and_failover` (per-key 1..12 ordered) | ✅ |
| FR-2.7 | Non-monotonic store rejected locally, counted; read-back never regresses | `Guard_rejects_non_monotonic_and_set_overwrites` (unit) + `Fallback_and_rejected_counters_emit_with_tags` (unit: `streambus_offset_store_rejected_total` emits) + failover/G-1 read-backs (stored == max received). Note: stale-instance path pinned at unit level; no broker-level stale-writer test | ✅ |
| FR-2.8 | Typed path deserializes via `IStreamSerializer` (default JSON); poison follows Retry/FailureMode | Unit `Serializer_round_trips` + `Attempts_resolve_fresh_and_poison_translates` (poison → `StreamBusDeserializationException`) + integration `Poison_skip_goes_to_error_stream`, `Poison_strict_never_skips`, `Type_mismatch_is_loud` | ✅ |
| FR-3.1 | Same routing value processed in publish order | `Super_stream_splits_across_instances_exactly_once_ordered` (per-key [1,2,3,4]) + `Typed_super_stream_ordered_per_key` + G-1/G-2 per-key groups | ✅ |
| FR-3.2 | Routing value is the only partition-selection input | Unit `Super_routing_passes_only_the_routing_value` (per-send equality: same key + different bodies agree, different keys preserved as-is) + integration `Super_stream_keys_land_on_stable_partitions` (same-key stability) | ✅ per Ruling R8: the library's FR-3.2 surface is the routing passthrough; partition selection itself lives in the client and is not pinnable deterministically — hash distribution is a client property, not a library behavior |
| FR-3.3 | Partition count immutable after declaration | `Super_partition_conflict_names_expected_and_actual` (integration: 5 vs 3 → conflict naming both) + unit `Partition_mismatch_throws_with_expected_and_actual` | ✅ |
| FR-3.4 | Hot-key ceiling (one partition's throughput per key) | N/A — docs-side by design (ceiling documented in design §4.5 + requirements §4; observability pinned by FR-9.3 test) | N/A (docs-side, stated explicitly) |
| FR-4.1 | Retry lives inside the handler; no external wrapping | `Strict_retry_window_survives_kill_without_skip` (kill mid-retry-window → nothing advanced) + order-under-retry tests (G-1, typed retry, EO partition order) + code fact: zero `Reliable*` references in `src/` (grep), `RawConsumer` is the only consumer factory | ✅ |
| FR-4.2 | Throwing handler never silently skipped; ordered paths avoid `Reliable` | `Handler_failure_redelivers_without_advancing` (attempts[2]==3, stored==4) + same no-`Reliable` code fact as FR-4.1 | ✅ |
| FR-4.3 | Every `RetryPolicy` knob exercised; 1-based attempts; `MaxAttempts` semantics | `Attempt_1_resolves_to_base_delay_without_jitter` (BaseDelay+Multiplier 200→400→800) + `Delay_caps_at_max_delay_and_jitter_stays_bounded` (MaxDelay+Jitter) + `Delay_calculator_overrides_knobs_with_one_based_attempts` (override + seen==[1,2]) + `Skip_with_max_attempts_1_hands_off_after_one_attempt` vs `Skip_handoff_publishes_envelope_and_advances_once` (MaxAttempts 1 vs 3) + `Strict_with_max_attempts_throws` (null-only-with-Strict) + bound-validation tests | ✅ |
| FR-4.4 | Strict stalls, never advances until success; Strict+ErrorStream rejected | `Strict_retry_window_survives_kill_without_skip` (redelivers same offset after kill) + `Strict_with_error_stream_throws` + `Strict_with_max_attempts_throws` | ✅ |
| FR-4.5 | Skip advances exactly once + error publish; hook observes only | Handoff pinned: `Skip_handoff_publishes_envelope_and_advances_once` (fields + stored==4) + `Skip_with_max_attempts_1_…` + `Skip_without_error_stream_throws` / `Skip_without_max_attempts_throws`. Hook pinned (Task 2): `OnRetryExhausted_observes_but_never_changes_outcome` (hook fires with full handoff fields; a throwing hook cannot change the outcome — both handoffs published, stream advances past poison) | ✅ |
| FR-4.6 | Failure mode is per consumer | `Strict_and_skip_modes_coexist_per_consumer` | ✅ |
| FR-4.7 | In-handler retry preserves ordering under load (G-1) | `Retry_preserves_order_under_load_and_failover` (24/24 distinct, per-key 1..12, stored == max per partition) | ✅ |
| FR-5.1 | Error message keeps body + all FR-5.1 fields | Unit `Round_trip_preserves_every_field_and_body` (every field + body) + `Skip_handoff_…` field asserts (source-stream/partition/offset, attempts, exception type/message/stack, failed-at, error-routing-value, body "k:2") | ✅ |
| FR-5.2 | Error stream has effective `x-max-age` retention | N/A — docs-side by design (effective retention is not readable via the stream protocol). Partial pins exist and are recorded: unit spec emission (`Default_spec_emits_no_retention_args`, 7d → `"604800s"`) + integration `Retention_declaration_is_usable` (retention-carrying declaration succeeds and works) | N/A (docs-side, stated explicitly) |
| FR-5.3 | Replay republishes with id from (partition, original offset); idempotent | `Replay_twice_stores_once` (1 stored; re-replay stores nothing new; garbage skipped) + unit `Replay_naming_is_stable_bounded_and_id_is_the_offset` | ✅ |
| FR-5.4 | Shared (default + custom routing) and per-stream error forms | `Shared_and_per_stream_error_forms_coexist` (default routing s1 + custom "custom-2", bodies distinct) + per-stream form in every skip test (dedicated error stream per source) | ✅ |
| FR-6.1 | Result publishing id = source offset (enforced by `PublishResultAsync`) | Unit `Publishing_id_equals_source_offset` + `Plain_to_plain_maps_publishing_id_to_source_offset` (`QuerySequence`==4 after offsets 0..4) | ✅ |
| FR-6.2 | Exactly one write per step; target publish is the only side effect | `Kill_mid_run_resumes_without_dupes_or_gaps` (exactly-10 convergence) + `Stable_producer_is_reused_and_carries_source_offset_as_id` + inspection (plain path returns; only result path publishes) | ✅ |
| FR-6.3 | One publisher per producer name (SAC-enforced); N names for super sources | `Two_instances_share_one_group_without_corruption` (one SAC group, 8/8 exactly once) + unit `Duplicate_in_flight_id_fails_loudly` (concurrent same-id misuse rejected loudly) | ✅ |
| FR-6.4 | Stable, meaningful, per-(partition,target) names within 255/stream cap | Unit naming tests (stable/meaningful/differ-per-partition-and-target/≤255B) + `Producer_name_cap_is_enforced_per_target` (255 enforced per target, other targets unaffected) | ✅ |
| FR-6.5 | Results in order, none skipped; retry forever, never silent timeout | `Handler_retry_preserves_partition_order` (exact 0..4, attempts[2]==3) + unit `Fail_twice_then_confirms_three_publishes_one_return` + `Cancel_mid_handoff_throws_and_stops` | ✅ |
| FR-6.6 | Super target resumes from max last-id across all partitions | Unit `Resume_takes_max_across_super_partitions_and_skips_unknown` (+ null/empty cases) + `Plain_to_super_target_resumes_from_max` (8/8 distinct across resume) | ✅ |
| FR-6.7 | No offset tracking on the EO source path | `Plain_to_plain_…` asserts `GetConsumerOffsetAsync` null for the EO reference + `Super_target_without_routing_redelivers_and_writes_nothing` (no routing → redeliver, target untouched) | ✅ |
| FR-6.8 | Full read-process-write cycle correct end to end (G-2) | `Full_cycle_super_to_super_with_flakes_and_kill_G2` (20/20 distinct, per-key ordered, kill mid-run) | ✅ |
| FR-7.1 | Ensure* idempotent and cached | `Repeat_same_spec_makes_no_further_broker_calls` (unit: cached, probe count frozen) + `Repeat_and_concurrent_declares_converge` (integration: repeat no-op, streams usable) | ✅ |
| FR-7.2 | Concurrent declaration serialized; race tolerated, genuine conflict throws | Unit `Four_concurrent_same_spec_issue_single_create` + integration 4-way in `Repeat_and_concurrent_…` + conflict tests (`Partition_mismatch_…`, `Super_partition_conflict_…`, `Kind_mismatch_…`) + `Restart_with_identical_spec_succeeds` | ✅ |
| FR-7.3 | Leader locator defaults to `least-leaders` | Unit `Locator_defaults_to_least_leaders` + `Locator_maps_all_values` (declared-spec side; on-broker placement is G-3, open) | ✅ |
| FR-7.4 | Post-declaration tuning via policy, not queue args | Spec side pinned (`Default_spec_emits_no_retention_args`: default spec carries no tuning args). Operator path test exists (`Policy_applied_tuning_is_effective`) but is skip-guarded — runs only where the management API is reachable | ⚠️ partial — environment-gated, not a test gap (no Task 2 action; G-3-adjacent) |
| FR-7.5 | Specs are startup configuration, not per-publish | `Startup_declares_union_of_registrations` + `Nonblocking_startup_declares_in_background` + `Blocking_startup_declares_before_return` + `Missing_stream_fails_and_stays_absent` (publish never declares) | ✅ |
| FR-8.1 | One vhost per bus; no per-call override; duplicate registration fails; shared `StreamSystem` | `Duplicate_default_registration_fails`, `Duplicate_marker_registration_fails_but_distinct_markers_coexist`, `Default_and_marker_resolve_distinct_buses`, `Same_vhost_different_password_throws_naming_vhost`, `Buses_on_one_vhost_share_one_supervisor`, `Different_vhosts_get_different_supervisors` (no-override: API inspection — single `Vhost` on options, no per-call vhost) | ✅ |
| FR-8.2 | One bus serves many streams | `One_bus_serves_two_streams` (Task 2, integration: one bus publishes to two streams, both confirmed and delivered with bodies uncrossed — `a:0` on s1, `b:0` on s2) | ✅ |
| FR-8.3 | Supervised initial connect retries forever (capped backoff+jitter) | `Transient_failures_retry_then_ready` (unit) + `WaitUpTo_times_out_but_loop_keeps_retrying` (loop survives timeout → later Ready) + `Unreachable_broker_never_surfaces_terminal_error_and_recovers` (integration: no terminal error) + `Backoff_is_capped` | ✅ |
| FR-8.4 | Protocol/SSL/auth-mechanism/auth failures are fatal, never retried | `Fatal_error_becomes_fatal_without_retry` + `Protocol_ssl_and_auth_mechanism_errors_are_fatal` + `Every_authentication_failure_type_in_the_client_is_fatal` (reflection over every `AuthenticationFailure*` client type) | ✅ |
| FR-8.5 | Network errors retried indefinitely | `Transient_failures_retry_then_ready` + `Network_errors_are_not_fatal` + `Unreachable_broker_…` (state stays Starting/TransientFailure; host process alive) | ✅ |
| FR-8.6 | Per-call-site `ReadinessPolicy` (all three); wait bounded independently of confirm timeout | FailFast-at-publish pinned (`Not_ready_failfast_throws_not_ready`); wait-vs-confirm independence pinned both ways (`Confirm_timeout_throws_and_bus_stays_ready`, `Not_ready_…`). Call-site wait policies pinned (Task 2 + Task 3, unit, no broker): publish `WaitUpTo_times_out_at_publish_then_Indefinitely_waits` (WaitUpTo bounded-throw + Indefinitely success leg) + consume mirror `WaitUpTo_times_out_at_consume_call_site` (same `WaitUntilReadyAsync` machinery at the consume call site) | ✅ |
| FR-8.7 | `WaitUntilReadyAsync` reaches Ready/TimedOut/Fatal | `Transient_failures_retry_then_ready` (Ready) + `WaitUpTo_times_out_but_loop_keeps_retrying` (TimedOut) + `Fatal_error_becomes_fatal_without_retry` (Fatal) | ✅ |
| FR-8.8 | Health reports per-(vhost, bus type) state | `Healthcheck_pipeline_reports_per_vhost_bus_type_data` (unit: `/\|` + `orders-vh\|` keys with bus-type names) + `Health_reports_per_vhost_bus_type_state` (integration: Healthy + `/\|` key) | ✅ |
| FR-8.9 | Guest credentials fail at startup with actionable message | `Guest_credentials_fail_fast_with_actionable_message` (unit: names guest+loopback) + `Guest_at_startup_fails_with_actionable_message` (hosted startup) | ✅ |
| FR-8.10 | Pooling defaults 1/1, overridable per bus | `Pooling_defaults_are_one_and_overridable` + `Pool_upper_bound_rejected_with_named_limit` + `Out_of_range_pool_values_fail_fast_without_connecting` | ✅ |
| FR-8.11 | Non-blocking default vs blocking-with-timeout startup | `Non_blocking_startup_returns_without_waiting` + `Blocking_startup_succeeds_when_connector_answers` + `Blocking_startup_timeout_surfaces_timed_out` (+ `Caller_cancellation_propagates_as_canceled_not_timeout`) | ✅ |
| FR-9.1 | Per-partition metrics per §13 catalog | Pinned 7/7 (Task 2 closes the last two with `MeterListener` unit tests in the `ConsumerMetricsTests` style): lag (`Lag_observes_delivered_minus_stored` + `Retry_blocked_and_lag_…`), inflight (tracking-scope unit + `Inflight_gauge_tracks_dispatched_uncompleted`), retry_blocked (`Retry_blocked_and_lag_…`), `store_rejected_total` + `fallback_total` (`Fallback_and_rejected_counters_emit_with_tags`), `streambus_offset_regressed_total` (`Regressed_counter_emits_with_catalog_name`), `streambus_connection_state` (`Connection_state_observes_current_state`, observes Ready vs TransientFailure) | ✅ |
| FR-9.2 | Connection transitions observable for probes | `Transient_failures_retry_then_ready` (`TransientFailure` in `StateChanged` + `Ready`) + health tests (FR-8.8) | ✅ |
| FR-9.3 | Hot-key starvation detectable from metrics | `Retry_blocked_and_lag_expose_a_stalled_partition` (retry_blocked==1 + lag>0 on partition 0, both back to 0 after release) | ✅ |
| FR-10.1 | Single `ConsumeOffset`; `ResumeOrFirst` default resumes with no extra config | `Offset_branches_behave` e1 (default options resume: second drain exactly [3,4,5], no replay) | ✅ |
| FR-10.2 | Resume modes resolve from broker-stored offset on create/promote/reconnect | `Recreate_resumes_…` (create, fresh guard) + `Failover_…` (promote) + `Strict_retry_window_…` (post-kill re-attach from stored) | ✅ |
| FR-10.3 | No-stored-offset branches: First replays, Next tails, Fail throws naming ref+partition | Unit `No_stored_offset_falls_back` (First+Next) + `ResumeOrFail_without_stored_throws_naming_reference_and_partition` + integration e1/e2/e3 (`Offset_branches_behave`: resume, Fail-throws, AlwaysFirst replay, AlwaysNext tail) | ✅ |
| FR-10.4 | Broker read on every attach via supervision loop; reconnect during Strict retry resumes stored | `Strict_retry_window_survives_kill_without_skip` + unit `Query_failure_degrades_to_no_stored_branch` + code pointer (`StreamPartitionConsumer.OnPromotedCoreAsync` re-resolves on every promotion; `localWater` write-guard only) | ✅ |
| FR-10.5 | Resolved = stored+1; `Next` only via explicit `AlwaysNext` | `Stored_offset_resolves_to_next_and_broker_wins_over_local` (5 → `OffsetTypeOffset(6)`) + `Always_modes_ignore_the_broker` + site table: `OffsetTypeNext` only at `OffsetResolution.cs:21` (`AlwaysNext`) and `:34` (`ResumeOrNext` no-stored branch — explicit operator choice); promotion-failure fallback is `First` (`StreamPartitionConsumer.cs:945`), never implicit `Next`; `:203` only *disables SAC* for a `Next` resolution | ✅ |
| FR-10.6 | Stored == exact `context.Offset`; throwing handler never advances | `Handler_failure_redelivers_without_advancing` (attempts[2]==3, stored==4) + skip-case advance-once (stored==4 with poison at 2) | ✅ |
| FR-10.7 | Monotonicity per FR-2.7 | Same pins as FR-2.7 (guard unit + rejected-counter unit + failover/G-1 read-backs) | ✅ |
| FR-10.8 | Configurable cadence; default every message; redelivery window documented | `Every3_stores_at_3_and_redelivers_at_most_2` + `MaxInterval_timer_stores_last_success` + `Non_positive_every_messages/max_interval_throws` + defaults pinned (Task 2): `Offset_store_defaults_match_spec` (`new OffsetStoreOptions()` == `{ EveryMessages = 1, MaxInterval = null }`, incl. the `ConsumeOptions` default) | ✅ |
| FR-10.9 | Stores go through the per-partition consumer; super-stream entity never a store path | `Failover_preserves_offsets_and_order` (per-partition stored == max received; empty → null) + same read-back in G-1 test + API facts: `RawSuperStreamConsumer.StoreOffset` throws per client; `ConsumeContext` has no publish method (only `PublishResultAsync` publishes) | ✅ |
| FR-10.10 | Optional auto-derived `Reference`: deterministic, stable, unique per group, ≤255B | Unit `Derivation_is_stable_distinct_and_bounded` + `References_differ_per_consumer_type_but_stable` (two consumer types on one stream differ) + integration `Two_consumer_types_each_receive_everything` (distinct refs → both get all). Restart-stability follows from pure deterministic derivation (nothing persisted) | ✅ |
| FR-10.11 | Over-long `Reference` fails at config, not as broker `PreconditionFailed` | `Over_long_reference_throws_naming_limit` (300-char ref → config error naming 255) | ✅ |
| FR-10.12 | No-stored-offset fallback warns (ref, partition, branch, resolved) + metrics/health | Warning pinned (`No_stored_offset_falls_back` asserts ref+partition+branch in log; resolved type name logged per `OffsetResolution.cs:35-37`) + metrics pinned (`RecordFallback` emission with branch tag). **Health half not implemented**: health exposes only per-(vhost,bus) connection state (`Hosting.cs:660-683`; `DescribeStates` = Vhost/BusType/State) — no per-fallback health entry; fallback is observable via logs + `streambus_offset_fallback_total` only | ⚠️ — health clause exceeds implementation (Ruling R7, reword proposed below; user sign-off pending — `docs/2026-10-08-streambus-requirements.md` NOT edited). Proposed reword: "Whenever the library falls back because the broker has no stored offset, it logs a warning naming the reference, the partition, the branch taken and the resolved offset, and publishes it to the metrics surface." |
| FR-10.13 | `Get/SetConsumerOffsetAsync` escape hatches; exact values; not on hot path | `Set_then_get_round_trip_and_resume` (set 7 → get 7 → resume delivers [8,9]) + `Missing_stream_throws_not_found_on_all_entry_points` + `Bus_level_set_audit_reaches_a_real_logger` (Set audit log; Set bypasses guard by design). Hot-path exclusion: code pointer (Get/Set live on registry/bus; per-message path uses guard + partition consumer only) | ✅ |
| FR-10.14 | EO source stores nothing; publishing id is the resume point | `Plain_to_plain_…` asserts `GetConsumerOffsetAsync` null for the EO reference | ✅ |

### Candidate further gaps for the controller (resolved by Task 2 + Rulings R7/R8)

All five Task 1 candidates are closed — each was a genuine per-discipline gap
and each now has its pin or ruling (verified green this session, see verdict):

1. **FR-8.6 call-site wait policies → ✅.** Publish:
   `WaitUpTo_times_out_at_publish_then_Indefinitely_waits`; consume mirror
   (Task 3): `WaitUpTo_times_out_at_consume_call_site`.
2. **FR-9.1 unpinned series → ✅ 7/7.** `Regressed_counter_emits_with_catalog_name`
   + `Connection_state_observes_current_state` (`MeterListener` style).
3. **FR-10.8 defaults → ✅.** `Offset_store_defaults_match_spec`.
4. **FR-3.2 distinctness/only-input → ✅ per Ruling R8** (library surface =
   routing passthrough; hash distribution is a client property, not pinnable).
5. **FR-10.12 health-surface clause → ⚠️ per Ruling R7** (reword proposed in
   the FR-10.12 row; requirements doc untouched pending sign-off).

Expected confirmations verified (not assumed): FR-8.2 now pinned by
`One_bus_serves_two_streams`;
FR-4.5 hook now pinned by `OnRetryExhausted_observes_but_never_changes_outcome`;
FR-10.9 IS pinned (failover per-partition stored == max received);
FR-5.1 IS pinned (envelope round-trip + handoff field asserts);
FR-4.3 numbering IS pinned (`Delay_calculator_overrides_knobs_with_one_based_attempts`,
seen==[1,2]).

## Non-functional verification

One subsection per NFR-1–NFR-7. Each names its evidence type; Task 3 outputs
pasted verbatim below (commands run from the repo root on 2026-10-13).

- NFR-1: ordering-first — evidence: ordering test roll-up (per-key-order
  tests: `Super_stream_splits_across_instances_exactly_once_ordered`,
  `Failover_preserves_offsets_and_order`,
  `Retry_preserves_order_under_load_and_failover`,
  `Typed_super_stream_ordered_per_key`, `Handler_retry_preserves_partition_order`,
  `Full_cycle_super_to_super_with_flakes_and_kill_G2` per-key groups,
  `Two_instances_share_one_group_without_corruption`) + Strict-by-default code
  pointer (`RetryPolicy` default `FailureMode.Strict`; `Strict` + `MaxAttempts`
  rejected). Final green lines (all roll-up members green — unit 179/179,
  integration 54 passed + 2 known env-skips):
  `Passed!  - Failed: 0, Passed: 179, Skipped: 0, Total: 179 - StreamBus.UnitTests.dll (net10.0)`
  `Passed!  - Failed: 0, Passed: 54, Skipped: 2, Total: 56 - StreamBus.IntegrationTests.dll (net10.0)`
- NFR-2: no database/scheduler/MassTransit — evidence: `dotnet list` package
  output (verbatim):
  ```
  > dotnet list src/StreamBus/StreamBus.csproj package
  Determining projects to restore...
  All projects are up-to-date for restore.
  Project 'StreamBus' has the following package references
     [net10.0]:
     Top-level Package                                    Requested   Resolved
     > Microsoft.Extensions.Diagnostics.HealthChecks      10.0.12     10.0.12
     > Microsoft.Extensions.Hosting                       10.0.12     10.0.12
     > Microsoft.Extensions.Logging.Abstractions          10.0.12     10.0.12
     > RabbitMQ.Stream.Client                             1.12.1      1.12.1
  ```
  Exactly the expected shape: `RabbitMQ.Stream.Client 1.12.1`,
  `Microsoft.Extensions.* 10.0.12`, nothing else — no MassTransit, no database
  client, no scheduler. (Test SDK/xUnit live on the test projects, not this
  library project.)
- NFR-3: library owns offset-on-promotion; no implicit First/Next — evidence:
  `OffsetTypeFirst`/`OffsetTypeNext` site table with per-site justification,
  verified against the code (no site invented):
  ```
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern 'ReliableProducer|ReliableConsumer|ReliableSuperStream|Reliable[A-Z]\w*'
  (no matches — ordered paths never touch a swallowing layer)
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern 'OffsetTypeFirst|OffsetTypeNext'
  src/StreamBus/OffsetResolution.cs:20  AlwaysFirst → OffsetTypeFirst (explicit operator escape)
  src/StreamBus/OffsetResolution.cs:21  AlwaysNext → OffsetTypeNext (explicit operator escape)
  src/StreamBus/OffsetResolution.cs:34  no-stored branch: ResumeOrFirst → First else Next
      (explicit operator mode choice; warning :35-37 + onFallback metric :38)
  src/StreamBus/StreamPartitionConsumer.cs:203  `is not OffsetTypeNext` check disabling SAC
      for tail attaches (not a production)
  src/StreamBus/StreamPartitionConsumer.cs:903  comment: client falls back to Next on listener
      failure — guarded by the never-throw listener (:901-909, ignores args, re-queries broker)
  src/StreamBus/StreamPartitionConsumer.cs:945  promotion-degrade fallback to First
      (listener must never throw; loud via warning :940-943 + RecordFallback :944)
  ```
  First-run EO resume shape: `QueryStoredAsync` (`StreamPartitionConsumer.cs:914-917`)
  returns the target's last publishing id; never-published → null → the
  stored-null branch with the default `ResumeOrFirst` mode → First. Every
  production site fits one of the four sanctioned shapes; no implicit `Next`
  production exists.
- NFR-4: hot-key documented, not auto-solved — evidence: doc pointers
  (design §4.5, §11.3; requirements §4) + metrics visibility test
  (`Retry_blocked_and_lag_expose_a_stalled_partition`). Pointers stand; no
  command output to paste.
- NFR-5: automatic placement untouched — evidence: locator is a
  declaration-time knob passed through to the client spec, never a
  placement-API call by the library:
  ```
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern 'LeaderLocator|leader_locator|Placement'
  Hosting.cs:262,267,272,278          builder knob (default LeastLeaders) carried into the declaration
  TopologyRegistry.cs:190,193        knob folded into the declaration fingerprint (concurrency key)
  Topology.cs:3,16,26,51,62,66-70,   declaration shape + Map() to the client LeaderLocator passthrough
  81,90,93,96,109,112,114            + validation/conflict-naming
  ```
  No placement query/set API is ever called; the library only forwards the
  declared value. Locator mapping pinned by `Locator_defaults_to_least_leaders`
  + `Locator_maps_all_values`. On-broker placement is G-3 (OPEN, single node).
- NFR-6: no TTL/DLX/priority assumptions — evidence (verbatim):
  ```
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern '\bTTL\b|\bDLX\b'
  (no matches)
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern 'MassTransit'
  (no matches — not even prose mentions; stronger than the expected prose-only shape)
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern 'DeleteStream|DeleteSuperStream'
  (no matches — topology never deletes)
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern 'riority'
  (no matches — no priority assumptions anywhere)
  ```
  `CreateStream|CreateSuperStream` matches are the FR-7 sanctioned set plus two
  pattern overmatches (producer factories, not topology creation):
  `Connector.cs:15,16,24,25,89,104,124,128,131,135` (interface + pass-throughs),
  `TopologyRegistry.cs:50,93` (via-handle declaration calls),
  `ProducerRegistry.cs:138` (`CreateSuperStreamProducerAsync` — producer, not a stream),
  `ResultProducerRegistry.cs:202` (`CreateSuperStreamProducerWithReferenceAsync` — producer, not a stream).
- NFR-7: broker-owned offsets — evidence: FR-10 roll-up (FR-10.1–FR-10.14
  above) + no-local-resume code pointers (`QueryStoredAsync` /
  `OnPromotedCoreAsync` re-read on every attach; `localWater` write-guard
  only) + metric catalog match table (FR-9.1 support — every name in the
  design §13 catalog):
  ```
  > Select-String -Path 'src/StreamBus/*.cs' -Pattern 'streambus_[a-z_]+'
  StreamBusMetrics.cs:29  streambus_partition_lag
  StreamBusMetrics.cs:32  streambus_partition_inflight
  StreamBusMetrics.cs:34  streambus_partition_retry_blocked
  StreamBusMetrics.cs:37  streambus_connection_state
  StreamBusMetrics.cs:40  streambus_offset_store_rejected_total
  StreamBusMetrics.cs:41  streambus_offset_regressed_total
  StreamBusMetrics.cs:42  streambus_offset_fallback_total
  ```
  All 7 catalog names present, all 7 pinned (see FR-9.1).

## Gates

| ID | Gate | Evidence | Status |
|---|---|---|---|
| G-1 | Retry under load + failover preserves order | `Retry_preserves_order_under_load_and_failover` | PASS (green this session) |
| G-2 | Effectively-once end to end | `Full_cycle_super_to_super_with_flakes_and_kill_G2` | PASS (green this session) |
| G-3 | 3-node placement | `Leader_placement_gate_G3` (skip-guarded: proves the gate is wired, skips on single node) | OPEN (no cluster; single node confirmed by skip path, not by version probe) |
| G-4 | SAC failover offsets + ordering | `Failover_preserves_offsets_and_order` + `Super_stream_splits_across_instances_exactly_once_ordered` | PASS (green this session) |
| G-5 | Reconnect-during-retry + stale monotonicity | `Strict_retry_window_survives_kill_without_skip` + `Guard_rejects_non_monotonic_and_set_overwrites` (+ `Fallback_and_rejected_counters_emit_with_tags`) | PASS (green this session) |

## v1 verdict

v1 is done: every FR row is ✅ or accepted-⚠️ (FR-10.12 ⚠️ with the R7 reword
proposal pending sign-off; FR-7.4 ⚠️ environment-gated with its explicit reason;
FR-3.4/FR-5.2 N/A docs-side by design, stated explicitly) and G-1/G-2/G-4/G-5
are PASS — with G-3 open pending a cluster (no 3-node broker available; the
skip-guarded placement test proves the gate is wired and skips on the single
node; close the gate only on green non-skipped evidence from a cluster run).

Final green evidence (Release, broker localhost:5552, 2026-10-13):
- `dotnet test tests/StreamBus.UnitTests/StreamBus.UnitTests.csproj -c Release --nologo -v q`
  `Passed!  - Failed: 0, Passed: 179, Skipped: 0, Total: 179, Duration: 2 s - StreamBus.UnitTests.dll (net10.0)`
- `dotnet test tests/StreamBus.IntegrationTests/StreamBus.IntegrationTests.csproj -c Release --nologo -v q`
  `Passed!  - Failed: 0, Passed: 54, Skipped: 2, Total: 56, Duration: 23 s - StreamBus.IntegrationTests.dll (net10.0)`
  The 2 skips are exactly the known env-skips (verified by name):
  `TopologyIntegrationTests.Policy_applied_tuning_is_effective` (management-API
  gated) and `TopologyIntegrationTests.Leader_placement_gate_G3` (single node).
  No other failure or skip.

Directed verdicts applied (controller rulings): FR-3.2 → ✅ (R8, rationale in
row); FR-10.12 → ⚠️ "health clause exceeds implementation" with the reword
proposal in-row (R7, user sign-off pending; requirements doc untouched).
