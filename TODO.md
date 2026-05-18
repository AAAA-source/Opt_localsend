# HopSwift v3 — Test Plan & Open Work

This branch (`main`, commit `f2307ab1`) adds:

- **P0** — visible progress UI for swarm (HopSwift v3) sessions + timing
  instrumentation (`SENDBENCH` / `RECVBENCH` log lines, on-screen timing
  card, per-peer completion times, chunks-by-source breakdown).
- **P1** — A/B benchmark page that runs v2 multi-send and v3 swarm back to
  back over the same payload and appends a CSV row per run.

Nothing has been verified to compile (no Flutter SDK on the dev box). All
items below assume a Flutter SDK that matches `.fvmrc` (3.38.10).

---

## 0. Build & static checks (do this first)

- [ ] `cd app && dart pub get`
- [ ] `dart run build_runner build --delete-conflicting-outputs`
- [ ] `dart analyze` — must come back clean. Likely failure points if any:
  - refena `Consumer` builder signature (2 args, not 3)
  - import paths in `pages/swarm_progress_page.dart`,
    `pages/benchmark_page.dart`, `provider/network/swarm/swarm_benchmark.dart`
  - new `copyWith` fields on `SwarmSendState` / `SwarmReceiveState`
- [ ] `dart format .` (I did not run it)
- [ ] App boots; no red screen on Send / Settings tabs

---

## 1. Smoke test (single machine)

- [ ] Settings tab → enable **Swarm mode (experimental)**
- [ ] Send tab with **0 files / 0 devices** — "Swarm send to all" hidden,
      "Run A/B benchmark" hidden
- [ ] Send tab with **1 file + 1 device** — both hidden (needs ≥2 devices)
- [ ] Send tab with **1 file + ≥2 devices** — both buttons visible

---

## 2. P0 — Swarm progress UI

Setup: 1 sender + ≥2 receivers (any mix of phones/desktops). Receivers
should run the same build with swarm mode enabled.

### Sender side

- [ ] Tap **Swarm send to all** → app navigates to `SwarmProgressPage`
      *immediately* (not after hashing finishes)
- [ ] **Phase A**: header shows "Preparing (hashing)" + 0→100% progress bar
- [ ] **Phase C**: header changes to "Direct uploads (sender → peers)";
      bar's denominator is total chunk count across all files
- [ ] **Targets** list shows each device; trailing column shows `…` while
      pending and `x.x s` once that peer's bitmap reports complete
- [ ] On completion, **Timing card** appears with three rows:
      `Prepare (hash) / Send window / Last-receiver done`
- [ ] Closing the page does not crash; reopening from the send tab still
      shows the finished state (until session is closed)

### Receiver side

- [ ] On accept, app navigates to `SwarmProgressPage` (the **swarm** page,
      not the v2 ProgressPage). Header: "Receiving from `<alias>`"
- [ ] Total byte progress bar advances smoothly
- [ ] Per-file progress shows `received/total chunks`
- [ ] **Chunks by source** section lists `sender` and `peer xxxxxxxx`
      entries with chunk counts. With ≥2 receivers there *should* be at
      least some peer chunks (otherwise the swap mechanism isn't kicking
      in — flag this).
- [ ] On completion, Timing card shows `First chunk / Last chunk / Wall clock`

### Log lines (grep from device logs)

- [ ] Sender emits exactly one
      `SENDBENCH,<sessionId>,swarm,<M>,<totalBytes>,<totalChunks>,<prepareMs>,<sendMs>,<lastReceiverMs>,<peerTimings>`
- [ ] Each receiver emits exactly one
      `RECVBENCH,<sessionId>,<totalBytes>,<totalChunks>,<firstMs>,<lastMs>,sender=N;<peer>=N`
- [ ] `peerTimings` in SENDBENCH has one entry per accepted target, all
      non-negative

---

## 3. P1 — A/B benchmark

Setup: same as P0. Receivers should have **Quick save** on or be ready to
accept twice in quick succession (once for v2, once for v3).

- [ ] Tap **Run A/B benchmark (v2 then v3)** → opens `BenchmarkPage`
- [ ] Payload / Targets summary at top matches what was selected
- [ ] Tap **Start** → button shows spinner, "Phase: v2" appears
- [ ] On phase change, "Phase: v3" appears; sender's `SwarmProgressPage`
      does *not* push (benchmark drives the providers directly)
- [ ] Result card appears: v2 seconds, v3 seconds, v2/v3 ratio, CSV path
- [ ] Open the CSV path (long-press to select-and-copy on mobile). File
      should contain a header row plus one row per run. Schema:

      `timestamp,targets,files,totalBytes,v2Ms,v3Ms,speedup_v2_over_v3,v2Error,v3Error`

- [ ] Run **at least three** payload sizes — e.g. 100 MiB, 1 GiB, 9 GiB
      (the proposal's reference number). Record results in the CSV.
- [ ] Run with **2 receivers** and **3 receivers**, separately

### What the numbers should show (and why we care)

- v3 should beat v2 on completion time once `M ≥ 2`, growing with `M`.
  If v3 is *slower*, check:
  - `SENDBENCH.prepareMs` dominating (hashing on UI isolate — known
    limitation, item 5 below)
  - `RECVBENCH.sender=N;peer=0` for every receiver (peer-swap never
    triggered — bitmap gossip dropping packets, or peer pull worker not
    starting)

---

## 4. Edge cases

- [ ] **All receivers decline** → sender ends with `declined` status,
      no crash, page closable
- [ ] **One receiver disconnects mid-transfer** → other receivers keep
      going via altruistic fallback; finished receivers show in the
      Targets list; sender log marks the dead peer's `peerTimings` at
      the session end time
- [ ] **v2 session active while another peer tries v3** (or vice versa)
      → `/v3/prepare-swarm` returns 409 "Blocked by another session"
- [ ] **Web-only file (no path)** → swarm skips it with a warning;
      should not crash

---

## 5. Known limitations / next work (not in this branch)

In rough priority order:

1. **Hashing runs on UI isolate** — `chunk_planner.planChunks` is
   awaited inline in `swarm_send_provider.startSwarmSession`. On a 9 GiB
   file this freezes the UI for tens of seconds. Move to `Isolate.run`.
2. **No unit tests** — the user has someone else writing these.
   Highest-value targets:
   - `common/lib/src/task/swarm/chunk_planner.dart` — boundary chunk
     sizes (`fileSize % chunkSize == 0` vs `!= 0`), single-chunk file,
     0-byte file, hash determinism
   - `common/lib/model/dto/swarm/bitmap_dto.dart` — `has` / `receivedCount`
     across byte boundaries; `empty(totalChunks=0..N)` byte length
   - `swarm_receive_provider.writeChunk` — hash mismatch path, duplicate
     chunk path, full-file SHA verify on small fixture
3. **Per-chunk file open/close on sender** — every uploaded chunk does
   `File.open / setPosition / read / close`. Cache an open `RAF` per
   file (the receiver already does this).
4. **No discovery-level v3 capability flag** — `protocolVersion` is
   still 2.1; v3 support is probed by hitting `/v3/prepare-swarm`. A
   capability bit in `/info` would let the sender skip the round trip.
5. **Sender RAF pool / read-ahead** — proposal slide 17 ("Ring buffer:
   fast read/write") not yet implemented.

---

## 6. Reporting back

When you're done, please send:

- `dart analyze` output (or "clean")
- The CSV file from `<cacheDir>/hopswift_bench.csv`
- A short note on whether peer-swap actually happened (any
  `RECVBENCH` row with a non-`sender` source counted > 0)
- Any crash logs / screenshots
