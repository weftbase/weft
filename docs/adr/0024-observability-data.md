# ADR 0024 — Observability data: OTel out, one identity chain

- Status: decided (2026-09-30). Moved to Weftbase with the platform in
  0.21: the full text is `docs/adr/0024-observability-data.md` in
  github.com/weftbase/weftbase (and in this repository's history before
  commit `312382c2`).
- Specification: `WEFT-OTEL-DATA-ARCHITECTURE.md` (Part II, steps
  S1–S7), in the research checkout; the "S" numbers weft's code cites
  are its steps.

The platform half of this ADR (`obsdb`, `studio`, `runtime`, the
ClickHouse backend, hosted Studio) left with the platform. The
decisions below still bind code in this repository; comments citing
"ADR 0024 Dn" or "ADR 0024 Sn" resolve here, and the record they shape
is specified in `docs/record-contract.md`.

## Decisions that still apply to weft

- **The direction.** OpenTelemetry is the only way observability data
  leaves the process: the core reports spans (ADR 0016) and emits the
  run's events and transcript as OTel log records; `weft/otel` wires
  the destinations, each with its own content policy. The core imports
  the OTel trace **and logs** APIs and nothing else (ADR 0016's one
  permitted dependency, widened here); a program with no SDK pays no
  encoding.
- **D1 — the run's repaired input is the first `messages` record**, and
  the transcript is never capped (caps apply to event bodies and
  deltas). Amended by ADR 0028 §8 for resumes.
- **D2 — capture is signalled through the Logs API's `Enabled`**, not a
  provider interface: the core asks `Enabled(EventName:
  "weft.messages")` at each emission; `MaxBytes` and `Redact` live per
  destination in `weft/otel`, which alone reads the standard OTel
  environment variables.
- **D3 — two counters.** `weft.event.pos` numbers the seven durable
  events contiguously from 0 per run; `weft.delta.pos` numbers deltas;
  `Nested` is not a record (the child's own run records itself).
- **D5 — per-run configuration is dual options plus RunOptions, no
  config struct.** `Instructions`, `MaxSteps`, `Parallelism` (like
  `Thinking`, `ToolChoice`, `Params`) are Option and RunOption at once;
  `OnlyTools` and `UseModel` are RunOptions. `MaxSteps`/`Parallelism`
  only lower per run, `OnlyTools` only narrows: a raise or an unknown
  name is `ErrInvalidRunOption` before any model call. Per-run
  configuration is not a seam (ADR 0006 and ADR 0007 unchanged).
- **D7 — breakpoints are a per-run `ParkOn`**: the ADR 0007 approval
  boundary applied per run, since an agent is immutable after `New`.
  Amended 2026-10-02 with `ParkAllExcept` (default-deny, inherited by a
  child run, never parking the agent's own `Output` answer) and by ADR
  0030 (`ParkSource`).
- **Content is off by default in the core** (ADR 0016 O7); `weft/otel`
  turns it on per destination, and `weft.StripContent` is the one
  shaping table (S1.4). App-chosen names (a session title, a label) are
  metadata, not content (amendment 2026-10-19).
- **The identity chain** (S1.1, S1.2, S5): `weft.public_id` →
  `weft.session.id` → `weft.run.id` → `weft.step.index` →
  `weft.tool.seq` → `weft.event.pos` / `weft.delta.pos`, with
  `weft.manifest.hash` + `weft.version` naming the agent's
  configuration, `Metadata` pairs on every span and record (S1.1), the
  `weft.override.*` fingerprint of a changed run (S1.2), and
  `weft/thread`'s session identity on every turn's run (S5). The
  attribute tables are `docs/record-contract.md`'s.
