package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"maps"
	"reflect"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/weftbase/weft/core/internal/contentflag"

	"go.opentelemetry.io/otel/attribute"
)

// rblock accumulates one provider reasoning block. A block stays open
// until a delta carries its signature (providers send the signature
// last); the next reasoning delta then starts a fresh block.
type rblock struct {
	text strings.Builder
	sig  string
	open bool
}

// execute runs the agent loop: model call, tool fan-out, repeat until the
// model stops requesting tools or the step budget runs out. Every notable
// moment is reported through emit, which must be safe for concurrent use.
//
// A panic in user code the loop calls on this goroutine is the run's
// failure, never a crash (ErrRunPanicked): runLoop's guard contains
// every panic once the run span has started; this outer guard contains
// the rest — user code reached before the span exists (a model's Info,
// a UseModel run's middleware constructors) — as a *RunError with no
// span to end and no OnRunEnd, like any failure before the run starts.
// Stream runs execute on a goroutine of its own, where nothing of the
// caller's could recover; Generate gets the same error (R14).
func (a *Agent) execute(ctx context.Context, cfg runConfig, sink func(Event)) (res *RunResult, err error) {
	defer func() {
		if p := recover(); p != nil {
			res, err = nil, &RunError{Err: &PanicError{Value: p, Stack: debug.Stack()}, Result: &RunResult{ID: cfg.id}}
		}
	}()
	return a.runLoop(ctx, cfg, sink)
}

// runLoop is execute's body; its result is execute's.
func (a *Agent) runLoop(ctx context.Context, cfg runConfig, sink func(Event)) (outRes *RunResult, outErr error) {
	// This agent now runs on this context: the ancestry chain grows by
	// one per nesting level, and a Subagent handler refuses a delegation
	// whose child is already on it (the cycle guard, ADR 0014).
	ctx = withAncestry(ctx, append(slices.Clone(ancestryOf(ctx)), a))
	// A run started on a model chain's context (a middleware running a
	// child agent) does not report into that chain's step: its handlers
	// see no reporter, its own model calls install theirs.
	ctx = maskReport(ctx)
	// The run's linkage is read once, from the context it was started
	// on (runLinkAttrs: a Subagent's child names its parent, any other
	// run on a tool call's context its caller), and the call it was
	// started on is then marked as this run's own: inside the run — its
	// model chain, PrepareStep, taps, OnRunEnd — that call is the one
	// the run serves, not one it dispatched, until a step installs a
	// call of its own. A run started there (a judge's Generate in a
	// model middleware) is started from no call this run made: it
	// carries no link, as under a top-level run, and never claims the
	// Subagent's parent link (O11, record contract §2).
	link := runLinkAttrs(ctx)
	callerSpan := remoteCallerSpan(ctx)
	ctx = enterRun(ctx)
	// What a tool call's context carries for its own handler — the
	// dispatch gate and hook, ParkUnknown, the park rule, the OnlyTools
	// narrowing, the inner-dispatch keying — is that run's and that
	// call's alone: a run started on it (a Subagent's child, a Generate
	// in a handler or a tool middleware) starts from none of it.
	ctx = clearCallScope(ctx)
	// The run's metadata is merged and placed before any span starts, so
	// every span and record of this run carries it and a Subagent's child
	// run inherits it through the tool call's context (ADR 0024 S1.1).
	md, mdDropped := mergeMetadata(metadataFromCtx(ctx), cfg.metadata)
	if n := metadataDroppedFromCtx(ctx); n > 0 {
		// What WithMetadata dropped is this run's to report, once: its
		// children start from a cleared count.
		mdDropped += n
		ctx = context.WithValue(ctx, metadataDroppedKey{}, 0)
	}
	if md != nil || metadataFromCtx(ctx) != nil {
		// nil too when an own empty value removed every inherited pair.
		ctx = withMetadata(ctx, md)
	}
	// The run's model: the agent's, or — when UseModel replaced it — the
	// WrapModel chain rebuilt over the alternate, first registered
	// outermost, exactly as New builds it (ADR 0024: configuration the
	// run carries, not a seam). Every model call, span and RunStart of
	// this run resolves against this chain.
	model := a.model
	// Under the development detail level (otel.DevDetail) the chain has
	// a layer span boundary outside each layer (layers.go): the agent's
	// built once, a UseModel run's with its rebuild. Asked only when
	// there is a chain to detail.
	detail := len(a.modelMW) > 0 && a.obs.devDetailMarked()
	if detail && cfg.model == nil {
		model = a.detailModel()
	}
	if cfg.model != nil {
		model = cfg.model
		for i := len(a.modelMW) - 1; i >= 0; i-- {
			model = a.modelMW[i](model)
			if isNilModel(model) {
				return nil, &RunError{Step: 0, Err: fmt.Errorf("%w: model middleware returned a nil Model", ErrModelContract), Result: &RunResult{ID: cfg.id}}
			}
			if detail {
				model = &layerModel{next: model, name: describeModel(model).Name, index: i}
			}
		}
	}
	// The run span's own linkage and provenance: a subagent's child run
	// names its parent (the call it executes under), a named agent its
	// manifest hash, every run the core version, and a run whose metadata
	// hit the limits the drop count (ADR 0024 S1.2). A run that changed
	// its configuration carries the experiment's fingerprint
	// (weft.override.*).
	// The raw configured instructions' hash (ADR 0028 §4): the agent's
	// Instructions or the run's override, before PrepareStep and before
	// composition — fixed for the run, always present (sha256 of "" when
	// there are none). One hash per run, on RunStart, its record and the
	// invoke_agent span; it is not content. The agent's own is hashed
	// once at New; only a run override is hashed here.
	instructionsHash := a.instructionsHash
	if cfg.systemSet {
		instructionsHash = hashText(cfg.system)
	}
	// A resume's pending calls — the calls the earlier run left without
	// a result — are found before the run span starts: the span and the
	// run_start record report each one's decision (weft.resume.calls),
	// beside the run ResumeOf named (weft.resumed_from).
	var resume []ToolCallPart
	var skip map[string]bool
	if len(cfg.decisions) > 0 {
		resume = unresolvedCalls(cfg.messages)
		skip = make(map[string]bool, len(resume))
		for _, c := range resume {
			skip[c.ID] = true
		}
	}
	resumeCalls := resumeCallsAttr(resume, cfg.decisions)
	var runExtra []attribute.KeyValue
	runExtra = append(runExtra, cfg.overrideAttrs(a.params)...)
	if resumeCalls != "" {
		runExtra = append(runExtra, attrResumeCalls.String(resumeCalls))
	}
	if cfg.resumedFrom != "" {
		runExtra = append(runExtra, attrResumedFrom.String(cfg.resumedFrom))
	}
	runExtra = append(runExtra, attrInstructionsHash.String(instructionsHash))
	runExtra = append(runExtra, link...)
	if a.manifestHash != "" {
		runExtra = append(runExtra, attrManifestHash.String(a.manifestHash))
	}
	if a.outputType != "" {
		runExtra = append(runExtra, attrOutputType.String(a.outputType))
	}
	runExtra = append(runExtra, attrVersion.String(version))
	if mdDropped > 0 {
		runExtra = append(runExtra, attrMetadataDropped.Int(mdDropped))
	}
	// The run's own reporting begins here: one invoke_agent span, on this
	// context, so every chat, execute_tool and tap below parents under
	// it, ended by every exit with the outcome decided — including
	// cancellation, which no event reports (ADR 0016).
	// The recorder is built before the span so the span's end reads its
	// counters: weft.run.events / weft.run.messages, the records it
	// handed to the LoggerProvider (record contract §3).
	records := recorder{
		elog:         a.obs.elog,
		capture:      a.content,
		runID:        cfg.id,
		agent:        a.name,
		manifestHash: a.manifestHash,
		resumedFrom:  cfg.resumedFrom,
		resumeCalls:  resumeCalls,
		link:         link,
		callerSpan:   callerSpan,
		obs:          &a.obs,
		panics:       &a.tapPanics,
	}
	ctx, endRunSpan := a.obs.run(ctx, cfg.id, a.name, InfoOf(model), runExtra, &records)
	// Content(false) rides the run's context for mw.Audit (the error
	// text it would log is content); a child run of another choice lifts
	// its parent's mark. Nothing is added when neither applies.
	if off := a.content != nil && !*a.content; off || contentflag.Off(ctx) {
		ctx = contentflag.With(ctx, off)
	}
	// An Output agent's run says whether it ended with a valid
	// submission (weft.output.submitted, UX-S1-06): GenerateAs's
	// ErrNoOutput is the caller's, the span says it happened.
	endSpan := func(r *RunResult, err error, end runEnd) {
		if a.hasOutput {
			_, ok := submittedJSON(r)
			end.output, end.submitted = true, ok
		}
		endRunSpan(r, err, end)
	}
	// A panic nothing nearer contains — PrepareStep, a ToolSource read,
	// a StopWhen condition, a SteerFunc are arbitrary user code; model,
	// tool, and tap panics are contained further down — is the run's
	// failure (ErrRunPanicked): this guard fails the run through fail,
	// so the span ends with error.type run_panicked, OnRunEnd sees the
	// *RunError and the partial transcript rides on it (ADR 0016,
	// amendment 2026-11-10). It never re-panics: under Stream this is
	// the producer's own goroutine, and a re-panic would end the process.
	// A runtime.Goexit (p == nil: a test's FailNow in user code) cannot
	// be stopped; the span still ends on its way out.
	var spanEnded bool
	var res *RunResult
	var failPanic func(step int, err error) (*RunResult, error)
	defer func() {
		p := recover()
		if res == nil {
			res = &RunResult{ID: cfg.id}
		}
		if p == nil {
			if !spanEnded {
				spanEnded = true
				endSpan(res, fmt.Errorf("%w: the run's goroutine exited", ErrRunPanicked), runEnd{})
			}
			return
		}
		pe := &PanicError{Value: p, Stack: debug.Stack()}
		if spanEnded {
			// The panic came while the run was ending (a user error's
			// own Is, read to classify the outcome): the span's end was
			// already under way; the caller still gets the run error.
			outRes, outErr = nil, &RunError{Step: len(res.Steps), Err: pe, Result: res}
			return
		}
		if failPanic == nil {
			spanEnded = true
			re := &RunError{Step: len(res.Steps), Err: pe, Result: res}
			endSpan(res, re, runEnd{})
			outRes, outErr = nil, re
			return
		}
		outRes, outErr = failPanic(len(res.Steps), pe)
	}()
	// Every event passes through here exactly once: the taps observe it
	// (synchronously, in emission order, on the emitting goroutine), the
	// recorder reports it as an OTel log record (durable events and
	// deltas on two counters; Nested is not reported — the child run
	// emits its own), then the sink receives it. Nothing is delivered
	// after cancellation — for taps, records and sinks alike, so
	// Generate and Stream agree and a consumer never observes a stream
	// that continues past its error. It reports whether the event was
	// delivered: the terminal RunFinish decides the run's outcome by
	// that answer, so a cancellation landing between a ctx check and the
	// emit can never produce a success without its RunFinish, or a
	// RunFinish followed by an error (rule 4; Run.Events'
	// one-terminal-element promise).
	deliver := func(ev Event) bool {
		if ctx.Err() != nil {
			return false
		}
		for _, tap := range a.taps {
			a.safeTap(ctx, tap, ev, &records)
		}
		records.recordEvent(ctx, ev)
		sink(ev)
		return true
	}
	emit := func(ev Event) { deliver(ev) }
	// The input transcript is repaired before the first model call, so
	// anything a caller feeds back in (a partial transcript, a resumed
	// session) becomes valid provider input. When the caller supplies
	// approval decisions, the calls left pending by the earlier run are
	// exempt from repair: this run resolves them itself, below (resume
	// and skip are computed before the run span starts, which reports
	// them as weft.resume.calls).
	res = &RunResult{ID: cfg.id, Messages: repair(cfg.messages, skip)}
	seq := new(atomic.Int64)
	// The OnRunEnd observers report the run's outcome exactly once —
	// endRun's undelivered-RunFinish path routes through fail, and the
	// run must read as ended once, with the failure. A contained panic
	// (the guard above) is such a failure: they see its *RunError.
	observed := false
	observeRunEnd := func(err error) {
		if observed {
			return
		}
		observed = true
		for _, fn := range a.onRunEnd {
			a.safeRunEnd(ctx, fn, res, err)
		}
	}
	// held is the input's tail a resume will rebuild or extend (below):
	// recorded after the resumed results attach, or here, if the run
	// fails before they do — so the messages records still concatenate
	// to the transcript (ADR 0024 D1, ADR 0028 §8). It is accepted input,
	// like record 0, so it is recorded even when the run's context has
	// been cancelled (WithoutCancel): a cancelled resume's records still
	// rebuild RunError.Result.Messages.
	var held []Message
	fail := func(step int, err error) (*RunResult, error) {
		if held != nil {
			records.recordMessages(context.WithoutCancel(ctx), 0, held, false)
			held = nil
		}
		spanEnded = true
		re := &RunError{Step: step, Err: err, Result: res}
		endSpan(res, re, failEnd(ctx, err))
		observeRunEnd(re)
		return nil, re
	}
	failPanic = fail
	// Every successful exit ends here — four sites share the shape, and
	// a field added to RunFinish must not miss any of them. One check
	// decides both the event's delivery and the run's outcome: a
	// delivered RunFinish is always followed by success, an undelivered
	// one (the run's ctx ended first) always by the cancellation error,
	// with the resumable result riding on it — cancellation wins over
	// success at the approval boundary too (ADR 0007). The caller sets
	// res.Pending first; snapshotPending(nil) is nil, so ordinary exits
	// carry no pending list.
	//
	// by names what ended the run (RunFinish.StoppedBy,
	// weft.run.stopped_by): a fired StopWhen condition's manifest name,
	// stoppedFinalAnswer or stoppedParked.
	endRun := func(step int, by string) (*RunResult, error) {
		if !deliver(RunFinish{RunID: cfg.id, Usage: res.Usage, Steps: len(res.Steps), Pending: snapshotPending(res.Pending), StoppedBy: by}) {
			return fail(step, ctx.Err())
		}
		spanEnded = true
		endSpan(res, nil, runEnd{stoppedBy: by})
		observeRunEnd(nil)
		return res, nil
	}
	// A model that can name itself does so on the first event; the
	// interface stays optional so Model remains one method.
	emit(RunStart{ID: cfg.id, Model: InfoOf(model), Agent: a.name, InstructionsHash: instructionsHash})
	// The input joins the record stream before anything else grows the
	// transcript: the repaired input, index 0, step 0 (ADR 0024 D1 —
	// today it is never reported at all, so a stored transcript could
	// not rebuild what the run was fed). An empty input (a run with no
	// messages) emits nothing.
	//
	// A resume attaches its results right after the last assistant
	// message with calls — rebuilding the partial tool message there,
	// or inserting a new one before anything that follows. The input
	// record stops at that assistant message, and what follows it is
	// recorded after the attach, as the next growth record: records
	// concatenated in index order stay the transcript, and a request's
	// messages_ref resolves to exactly its messages (ADR 0028 §3).
	inputRec := res.Messages
	if len(resume) > 0 {
		if i := lastAssistantWithCalls(res.Messages); i >= 0 && i+1 < len(res.Messages) {
			inputRec, held = res.Messages[:i+1], res.Messages[i+1:]
		}
	}
	records.recordMessages(ctx, 0, inputRec, true)
	if ctx.Err() != nil {
		// Cancelled before the input record went out: no record of this
		// run exists, so its tail is not recorded alone either.
		held = nil
	}

	// The approval boundary's second half: approved calls run now,
	// before any model call, and every other pending call is denied.
	// Their results complete the dangling tool message of the earlier
	// run, so the model sees an ordinary transcript.
	if err := ctx.Err(); err != nil {
		return fail(0, err)
	}
	// A Resolve carries a payload the caller expects the model to
	// see: one aimed at a call that is not pending is a programming
	// error, loud at step 0. The check runs even when nothing is
	// pending — an empty resume makes every resolved id a breach, and
	// the check living inside the resume block used to drop such
	// Resolves silently instead (review 2026-09-24 §2.1). Approve and
	// Deny keep ignoring unknown ids — the deliberate asymmetry ADR
	// 0007's 2026-09-22 amendment records. Sorted, so the first breach
	// is deterministic when several ids miss. The common run carries
	// no decisions at all, so the pending-id set is only built when
	// there is something to check it against.
	if len(cfg.decisions) > 0 {
		pendingIDs := make(map[string]bool, len(resume))
		for _, c := range resume {
			pendingIDs[c.ID] = true
		}
		for _, id := range slices.Sorted(maps.Keys(cfg.decisions)) {
			if cfg.decisions[id].resolved && !pendingIDs[id] {
				return fail(0, fmt.Errorf("resolve: call %q is not pending in this run's transcript", id))
			}
		}
	}
	// Per-run configuration the agent refuses [D5]: a raised limit. The
	// check runs before any model call and before any resumed call, so a
	// misconfigured run does not start; errors.Is(err, ErrInvalidRunOption)
	// is the branch. Tool narrowing (OnlyTools) validates against the
	// step-0 snapshot just below, with the same rule.
	if cfg.maxStepsSet && cfg.maxSteps > a.maxSteps {
		return fail(0, fmt.Errorf("%w: max_steps %d raises the agent's %d; per run it may only lower", ErrInvalidRunOption, cfg.maxSteps, a.maxSteps))
	}
	if cfg.parallelismSet && cfg.parallelism > a.parallelism {
		return fail(0, fmt.Errorf("%w: parallelism %d raises the agent's %d; per run it may only lower", ErrInvalidRunOption, cfg.parallelism, a.parallelism))
	}
	// Narrowing validates against the run's first snapshot, before any
	// model call: an unknown name is a misconfiguration, not a run-time
	// surprise. A ToolSource that later drops a narrowed tool simply
	// stops offering it — calls to it fail as unknown, the standing
	// rule.
	if len(cfg.onlyTools) > 0 {
		snapshot, err := a.dispatchTools()
		if err != nil {
			return fail(0, err)
		}
		for _, name := range cfg.onlyTools {
			if _, ok := findTool(snapshot, name); !ok {
				return fail(0, fmt.Errorf("%w: tool %q is not registered on this agent; OnlyTools narrows, it cannot add", ErrInvalidRunOption, name))
			}
		}
	}
	// The run's tool fetch: the agent's snapshot narrowed to the named
	// subset. Advertising and dispatch both resolve against it, so what
	// the model was shown is exactly what runs (the one-snapshot rule).
	fetchTools := func() ([]*ToolDef, error) {
		tools, err := a.dispatchTools()
		if err != nil || len(cfg.onlyTools) == 0 {
			return tools, err
		}
		return narrowTools(tools, cfg.onlyTools), nil
	}
	// The run's effective configuration, resolved once: the loop reads
	// these, not the agent's fields, so a run-level option (the dual
	// Instructions/MaxSteps/Parallelism, the Thinking shape) reaches
	// every step of this run alone (ADR 0024: configuration the run
	// carries, not a seam).
	maxSteps := cfg.effectiveMaxSteps(a.maxSteps)
	parallelism := cfg.effectiveParallelism(a.parallelism)
	// The run's park rule (ParkOn, ParkAllExcept): the approval boundary
	// applied per run — a call the rule names parks exactly as
	// RequireApproval would (ADR 0007; ADR 0024 D7). An except-list in
	// force goes onto the context here, before any tool call's context
	// is derived from it, so the runs started inside this one inherit
	// it; nothing else reads it.
	//
	// The agent's ParkSources (ADR 0030) are read here, once: their names
	// join the run's ParkOn set. cfg is this run's copy, and the override
	// attributes were taken above, so a source's answer is never an
	// override.
	if len(a.parkSources) > 0 {
		cfg.parkSourced, cfg.parkSourceFailed = a.readParkSources(ctx, cfg.id)
	}
	ctx, parkSet := cfg.resolvePark(ctx)
	res.Park = parkSet.export()
	if len(resume) > 0 {
		results, pending, sub, err := a.resolvePending(ctx, cfg, resume, seq, emit, fetchTools, parkSet, &records)
		if err != nil {
			return fail(0, err)
		}
		var joined []Message
		cut, tailHeld := len(res.Messages)-len(held), held != nil
		res.Messages, joined = attachResults(res.Messages, resume, results)
		if tailHeld {
			// The held tail as it is now: the rebuilt (or inserted)
			// tool message and whatever followed it — accepted input,
			// recorded through a cancellation (see held).
			held = nil
			records.recordMessages(context.WithoutCancel(ctx), 0, res.Messages[cut:], false)
		}
		if len(joined) > 0 {
			// The completed tool message — created, or rebuilt over one
			// the earlier run left partial — joins the transcript at
			// step 0 of the resume (ADR 0007 §3). The transcript
			// observers see it here, where the transcript grew, exactly
			// as they see every step's messages. Its record is the growth
			// record after the input: the held tail above when the input
			// continued past the assistant message (ADR 0028 §8), else
			// the joined message itself, below — so a resume's stored
			// transcript rebuilds too.
			a.observeMessages(ctx, cfg, 0, joined, &records)
			if !tailHeld {
				// Appended at the end: the next growth record.
				// Recorded through a cancellation, as the held tail is:
				// the joined message is accepted transcript either way.
				records.recordMessages(context.WithoutCancel(ctx), 0, joined, false)
			}
		}
		// Resumed delegations roll into the total only: there is no
		// StepRecord for resumed calls (ADR 0007), so no per-call map.
		res.Usage = rollUp(res.Usage, sub)
		if len(pending) > 0 {
			res.Pending = pending
			return endRun(0, stoppedParked)
		}
	}

	state := &loopState{retries: map[string]int{}}
	// A run that continues a turn (KeptSteps) spends what is left of the
	// turn's step budget, and its loop streak starts from the kept
	// steps' tool calls: an unchanged replay reaches the turn's stop.
	if k := cfg.keptSteps; k > 0 {
		maxSteps = max(maxSteps-k, 0)
		if a.detectLoops > 0 {
			for _, calls := range keptCalls(cfg.messages, k) {
				state.advance(stepSignature(calls))
			}
		}
	}
	for step := 0; step < maxSteps; step++ {
		if err := ctx.Err(); err != nil {
			return fail(step, err)
		}
		// One snapshot per step: advertising, the sequential barrier,
		// and dispatch all resolve against this fetch, so what the
		// model was shown is exactly what runs. A source with a
		// duplicate name fails here rather than silently dropping a
		// tool.
		tools, err := fetchTools()
		if err != nil {
			return fail(step, err)
		}
		emit(StepStart{RunID: cfg.id, Index: step})

		// req is the loop's own request for this step: what it records
		// and dispatches against. The model is handed its own copy at
		// the seam (modelRequest, below).
		req := ModelRequest{
			System: cfg.effectiveSystem(a.system),
			Tools:  tools,
			// A run-level Thinking option overrides the agent's default
			// for this run alone (the thinkingOption applies to both);
			// so do the dual Instructions/MaxSteps/Parallelism.
			SequentialTools: parallelism == 1,
			Thinking:        cfg.effectiveThinking(a.thinking),
			// The same dual-option rule for a forced tool choice.
			ToolChoice: cfg.effectiveToolChoice(a.toolChoice),
			// And for per-request sampling knobs: the agent's (or the
			// run option's) own values, never written through — the
			// chain and the model each get copies (cloneParams).
			Params: cfg.effectiveParams(a.params),
		}
		// The chain's result is compared (prepared[], below) against
		// these values: before keeps the step's own Params pointers,
		// which neither the chain nor the model can write through.
		before := req
		// PrepareStep functions are arbitrary user code, and the request
		// they see promises they may mutate it freely (ADR 0006
		// amendment): messages, tool definitions and the sampling
		// knobs' values are deep-copied at that boundary, so nothing a
		// function writes in place — a dropped part, re-formed argument
		// bytes, a rewritten description, *req.Params.Temperature —
		// reaches the run's transcript, the agent's frozen registry or
		// the agent's (or the caller's) Params. The model seam keeps
		// the lighter slice copies (requestMessages, modelRequest):
		// adapters hold the read-only request contract for the payload
		// bytes inside parts — under a chain, applied to what the chain
		// returns, so a message a function keeps is never the model's.
		if a.prepare != nil {
			req.Messages = cloneMessages(res.Messages)
			req.Tools = cloneTools(tools)
			req.Params = cloneParams(req.Params)
		} else {
			req.Messages = requestMessages(res.Messages) // adapters cannot reach the run's transcript
		}
		// The one loop knob: PrepareStep rewrites the request before the
		// model seam, on the raw instructions; what its chain returns is
		// what the step both advertises and dispatches against, so the
		// one-snapshot-per-step invariant holds by construction (ADR
		// 0006 amendment).
		for _, fn := range a.prepare {
			var err error
			req, err = fn(ctx, step, req)
			if err != nil {
				return fail(step, err)
			}
		}
		// What the chain changed (O9) is the request record's
		// prepared[], derived inside recordRequest only when a request
		// record is wanted (ADR 0016: nothing computed when nothing
		// records) — against the step's own inputs, the transcript and
		// the fetched tools, which the chain's deep copies leave
		// untouched.
		inputs := stepInputs{model: model, before: before, tools: tools, prepared: a.prepare != nil, parallelism: parallelism}
		if a.prepare != nil {
			// The chain's tools are the step's snapshot (without a
			// chain, the fetched one is). Either way the model is handed
			// a copy of the slice (modelRequest), so a model or
			// middleware that reassigns an element never changes what
			// this step dispatches or what the recorder remembers.
			tools = req.Tools
			if err := validateSnapshot(tools); err != nil {
				return fail(step, err)
			}
		}
		// A forced choice runs against the same snapshot the step
		// advertises and dispatches over — validated after the
		// PrepareStep chain, so a function that sets a name and drops
		// the tool fails in the same place. A programming error the
		// caller fixes, reported in the text, not a sentinel (ADR
		// 0013's 2026-09-22 amendment).
		if err := validateToolChoice(req.ToolChoice, tools); err != nil {
			return fail(step, err)
		}
		// Sampling knobs validated at the same point, so an agent
		// default, a run option, and a PrepareStep edit fail alike.
		if err := validateParams(req.Params); err != nil {
			return fail(step, err)
		}
		req.System = composeSystem(req.System, tools)
		var (
			sb       strings.Builder
			rblocks  []rblock // provider reasoning, one entry per block
			calls    []ToolCallPart
			finish   ModelFinish
			finished bool
		)
		// The model call's own reporting: one chat span around the chain —
		// PrepareStep has run and mw.Retry/mw.Fallback sit inside it, so
		// the span measures the chain's outcome (ADR 0016). The stream is
		// consumed on the span's context, so an adapter's own HTTP spans
		// parent under chat.
		info := InfoOf(model)
		mctx, endModel := a.obs.model(ctx, &records, cfg.id, step, info)
		// What this call is given, recorded beside it (ADR 0028): the
		// prompt and tools records when their hash is new to the run,
		// then the request record (attempt 1), on the chat span's
		// context, after PrepareStep, validation and composition, right
		// before the chain runs. Reporting only: req is not touched.
		reqRec := records.recordRequest(mctx, a, step, req, info, parkSet, res.Messages, &inputs)
		// The chain's reporting path (ReportFromContext): attempts and
		// wire bodies the chain reports land on this step's record; each
		// attempt after the first adds its own request record.
		rep := a.obs.withReport(mctx, cfg.id, step, &a.tapPanics)
		rep.request = reqRec
		rep.rec = &records
		mctx = rep
		// The Model stream contract (see Model) is enforced here, not just
		// documented: exactly one ModelFinish, nothing after it, and a
		// panicking implementation becomes a run error instead of crashing
		// the run goroutine — which no caller could recover.
		//
		// The call's timing is measured here as the stream is consumed
		// (ADR 0016's A4 note): a clock read at the start, one at the
		// first TextDelta/ToolArgsDelta, one at the end, kept on the
		// step's reporter (already allocated, so timing costs no
		// allocation) — and nothing the loop decides on.
		mreq := modelRequest(req)
		if a.prepare != nil {
			// The chain's Messages may hold a message a function keeps
			// across steps (a pinned instruction spliced in): the model
			// gets its own slices of those too, as it does without a
			// chain (requestMessages above).
			mreq.Messages = requestMessages(mreq.Messages)
		}
		consume := func() (err error) {
			rep.start = time.Now()
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("%w: model stream panicked: %v", ErrModelContract, p)
				}
			}()
			for mev, serr := range model.Stream(mctx, mreq) {
				if serr != nil {
					return fmt.Errorf("model stream: %w", serr)
				}
				if finished {
					return fmt.Errorf("%w: event %T after ModelFinish", ErrModelContract, mev)
				}
				switch e := mev.(type) {
				case ModelTextDelta:
					rep.firstDelta()
					emit(TextDelta{RunID: cfg.id, Text: e.Text})
					sb.WriteString(e.Text)
				case ModelReasoningDelta:
					emit(ReasoningDelta{RunID: cfg.id, Text: e.Text})
					// One ReasoningPart per provider block: a signature
					// closes the block (providers send it last), the next
					// delta opens a new one. Unsigned reasoning keeps
					// accumulating into the open block — adapters drop
					// unsigned blocks on send, so their boundaries cannot
					// corrupt a round trip.
					if len(rblocks) == 0 || !rblocks[len(rblocks)-1].open {
						rblocks = append(rblocks, rblock{open: true})
					}
					cur := &rblocks[len(rblocks)-1]
					cur.text.WriteString(e.Text)
					if e.Signature != "" {
						cur.sig = e.Signature
						cur.open = false
					}
				case ModelToolCall:
					if e.ID == "" {
						return fmt.Errorf("%w: tool call with an empty ID", ErrModelContract)
					}
					// A repeated ID within one step makes the transcript
					// ambiguous — repair keeps only the first result per
					// ID, and Approve/Deny key on it — so it fails the
					// run like an empty one. IDs may repeat across steps.
					if slices.ContainsFunc(calls, func(c ToolCallPart) bool { return c.ID == e.ID }) {
						return fmt.Errorf("%w: duplicate tool call ID %q", ErrModelContract, e.ID)
					}
					if e.Name == "" {
						return fmt.Errorf("%w: tool call with an empty name", ErrModelContract)
					}
					calls = append(calls, ToolCallPart(e))
				case ModelToolCallDelta:
					// Progress only — the assembled call still arrives
					// as a ModelToolCall before ModelFinish.
					rep.firstDelta()
					emit(ToolArgsDelta{RunID: cfg.id, Name: e.Name, Args: e.Args})
				case ModelFinish:
					finish = e
					finished = true
				default:
					return fmt.Errorf("%w: unexpected event %T", ErrModelContract, mev)
				}
			}
			if !finished {
				return fmt.Errorf("%w: stream ended without ModelFinish", ErrModelContract)
			}
			return nil
		}
		streamErr := consume()
		timing := callTiming{ttft: rep.ttft, latency: max(sinceAtLeast(rep.start), rep.ttft)}
		rep.end()
		// The model that answered (ADR 0016's A4 note): the last attempt
		// the chain reported as a success, else the model the call asked
		// for; only a call that finished has one.
		if finished && streamErr == nil {
			if timing.answered = rep.answeredModel(); timing.answered == "" {
				timing.answered = info.Name
			}
		}
		endModel(finish, finished, len(calls), streamErr, timing)
		if streamErr != nil {
			return fail(step, streamErr)
		}

		// An assistant turn with no text and no calls is not appended:
		// providers reject empty content, and a transcript that is fed
		// back into a later run must stay valid input. Reasoning counts
		// as content — a signed block the provider may expect back must
		// not be dropped — and is placed before the text, the order the
		// model produced it in.
		msg := Message{Role: RoleAssistant}
		for i := range rblocks {
			b := &rblocks[i]
			if b.text.Len() > 0 || b.sig != "" {
				msg.Content = append(msg.Content, ReasoningPart{Text: b.text.String(), Signature: b.sig})
			}
		}
		if text := sb.String(); text != "" {
			msg.Content = append(msg.Content, TextPart{Text: text})
		}
		for _, c := range calls {
			msg.Content = append(msg.Content, c)
		}
		if len(msg.Content) > 0 {
			res.Messages = append(res.Messages, msg)
			a.observeMessages(ctx, cfg, step, []Message{msg}, &records)
			records.recordMessages(ctx, step, []Message{msg}, false)
		}

		rec := StepRecord{
			Index:         step,
			StopReason:    finish.Reason,
			RawStopReason: finish.Raw,
			Usage:         finish.Usage,
			Text:          sb.String(),
			ToolCalls:     calls,
		}
		var pending []ToolCallPart
		var sub map[string]Usage
		switch {
		case len(calls) == 0:
		case finish.Reason == StopMaxTokens:
			// A message cut by the output-token limit must not have its
			// calls acted on: an intact-looking call may be the first
			// of several the model never finished issuing. Every call
			// fails without executing — a uniform, visible result — and
			// the loop continues so the model retries with a full
			// budget (ADR 0002, "fail truncated calls").
			for _, c := range calls {
				rec.Results = append(rec.Results, ToolResultPart{
					CallID:  c.ID,
					Name:    c.Name,
					IsError: true,
					Content: truncatedCallResult(c.Name),
				})
			}
		default:
			var retried []string
			rec.Results, pending, sub, retried = a.execTools(ctx, cfg.id, step, tools, calls, seq, emit, nil, parallelism, parkSet, &records, state.retries, cfg.keys, cfg.dispatch, cfg.afterDispatch, cfg.parkUnknown, cfg.onlyTools)
			for _, name := range retried {
				state.retries[name]++
			}
			for _, r := range rec.Results {
				if !r.IsError {
					delete(state.retries, r.Name) // a success resets the count
				}
			}
		}
		if len(sub) > 0 {
			rec.SubagentUsage = sub
		}
		if len(rec.Results) > 0 {
			toolMsg := Message{Role: RoleTool}
			for _, r := range rec.Results {
				toolMsg.Content = append(toolMsg.Content, r)
			}
			res.Messages = append(res.Messages, toolMsg)
			a.observeMessages(ctx, cfg, step, []Message{toolMsg}, &records)
			records.recordMessages(ctx, step, []Message{toolMsg}, false)
		}
		res.Steps = append(res.Steps, rec)
		res.StopReason = finish.Reason
		res.Usage = res.Usage.Add(finish.Usage)
		// Child-run usage rolls up after the step's own: StepFinish
		// reports the model call's numbers alone (finish.Usage), while
		// RunResult.Usage carries the whole bill, subagents included.
		res.Usage = rollUp(res.Usage, sub)
		records.stepModel = timing.answered
		emit(StepFinish{RunID: cfg.id, Index: step, Reason: finish.Reason, Usage: finish.Usage, Raw: finish.Raw,
			LatencyMS: ceilMS(timing.latency), TTFTMS: ceilMS(timing.ttft)})

		if len(pending) > 0 {
			// The approval boundary: the step's other tools have run;
			// the pending calls have no result in the transcript. The
			// run ends successfully and the caller resumes it with
			// Approve/Deny — unless the run was canceled, in which case
			// cancellation wins, as it does everywhere else: the run
			// fails with the ctx error, the parked calls riding on
			// RunError.Result.Pending so the transcript stays resumable.
			res.Pending = pending
			return endRun(step, stoppedParked)
		}
		// final marks the step the run would otherwise end on: the model
		// made no tool calls (a max_tokens finish with calls is not
		// final — its calls all carry error results and the loop
		// continues).
		final := len(calls) == 0
		if by, ok := a.stopped(res.Steps); ok {
			// An intended end stays an end: a StopWhen condition firing
			// ends the run here in every case, before the steering
			// drain below — a steer cannot un-end it (ADR 0019 §2).
			return endRun(step, by)
		}
		// The steering drain points (ADR 0019 §2): after a step's tool
		// batch, when every call of the batch has its result — success,
		// error, truncated, or denied — so the call/result pairing
		// cannot be split, and at a final step, where a delivered
		// message redirects the run into one more step instead of
		// ending. The boundary above and the StopWhen end just passed
		// are the two non-drain exits: the source keeps its messages
		// for a follow-up. The loop goroutine is alone here — tools
		// finished, the next model call not started — so the Seq below
		// cannot interleave with a tool event's.
		var steered []Message
		if cfg.steer != nil {
			steered = cfg.steer(ctx, SteerPoint{RunID: cfg.id, Step: step, Final: final})
			for _, m := range steered {
				if m.Role != RoleUser {
					return fail(step, fmt.Errorf("%w: role %q at step %d", ErrInvalidSteer, m.Role, step))
				}
			}
		}
		if final && len(steered) == 0 {
			return endRun(step, stoppedFinalAnswer)
		}
		if len(steered) > 0 {
			// Delivered messages are transcript (ADR 0019 §3): appended
			// before the next step's PrepareStep chain runs, so request
			// rewrites see them and the transcript stays the single
			// source of truth. The event is a snapshot — the transcript
			// owns the delivered messages, the event carries a copy.
			res.Messages = append(res.Messages, steered...)
			a.observeMessages(ctx, cfg, step, steered, &records)
			records.recordMessages(ctx, step, steered, false)
			emit(Steered{RunID: cfg.id, Seq: seq.Add(1), Step: step, Messages: cloneMessages(steered)})
		}
		// The continuation point: the loop is about to spend more, so
		// every budget is checked here. A step that ended the run above
		// succeeded even if it overshot — a budget stops further spend,
		// it does not discard finished work (ADR 0002). A redirect at a
		// Final point consumes a step and goes through the same checks
		// as any continuation; if they fail, the steer is in
		// RunError.Result.Messages, delivered but unanswered (ADR 0019
		// §5).
		if err := a.guard(res, &rec, state); err != nil {
			return fail(step, err)
		}
	}

	// Cancellation during the last allowed step's tools is reported as
	// cancellation, not as step exhaustion: the cause wins over the budget.
	if err := ctx.Err(); err != nil {
		return fail(max(maxSteps-1, 0), err)
	}
	// The model still wanted tools after its last allowed step. The
	// transcript, including the final step's tool results, rides on the
	// error.
	return fail(maxSteps, ErrMaxSteps)
}

// observeMessages runs the run's transcript observers (OnMessages) over
// one batch of messages that just joined the run's transcript — a deep
// copy, so an observer that retains or mutates its slice touches
// nothing the run sees. The fire sites are the transcript's growth
// points (the loop goroutine, between event emissions), so the calls
// come in transcript order without holding the event-ordering lock. A
// panicking observer is contained and counted like a tap: a broken
// observer must not break a run, but it must not be invisible either.
func (a *Agent) observeMessages(ctx context.Context, cfg runConfig, step int, msgs []Message, rec *recorder) {
	if len(cfg.onMessages) == 0 {
		return
	}
	snapshot := cloneMessages(msgs)
	for _, fn := range cfg.onMessages {
		a.safeOnMessages(ctx, fn, step, snapshot, rec)
	}
}

// safeOnMessages runs one transcript observer, containing a panic and
// counting it (TapPanics) — the same containment a tap gets.
func (a *Agent) safeOnMessages(ctx context.Context, fn func(context.Context, int, []Message), step int, msgs []Message, rec *recorder) {
	defer func() {
		if recover() != nil {
			if rec != nil {
				rec.countPanic()
			} else {
				a.tapPanics.Add(1)
			}
		}
	}()
	fn(ctx, step, msgs)
}

// safeTap runs one tap, containing a panic and counting it (TapPanics):
// a broken observer must not break a run, but it must not be invisible
// either.
//
// rec, when not nil, is the run's recorder: the panic is counted there
// (it adds to TapPanics too) for the run's weft.tap.panics.
func (a *Agent) safeTap(ctx context.Context, tap func(context.Context, Event), ev Event, rec *recorder) {
	defer func() {
		if recover() != nil {
			if rec != nil {
				rec.countPanic()
			} else {
				a.tapPanics.Add(1)
			}
		}
	}()
	tap(ctx, ev)
}

// safeRunEnd runs one OnRunEnd observer under Tap's containment rule:
// the outcome observer runs after the run is decided, so a panic in it
// cannot change the outcome either — it is contained and counted with
// the tap panics.
func (a *Agent) safeRunEnd(ctx context.Context, fn func(context.Context, *RunResult, error), res *RunResult, err error) {
	defer func() {
		if recover() != nil {
			a.tapPanics.Add(1)
		}
	}()
	fn(ctx, res, err)
}

// cloneRaw detaches a raw-JSON byte slice: json.RawMessage is mutable,
// and events are snapshots, so an event's Args must not alias the
// transcript's bytes.
func cloneRaw(b json.RawMessage) json.RawMessage {
	if b == nil {
		return nil
	}
	return append(json.RawMessage(nil), b...)
}

// snapshotPending deep-copies the pending calls carried on RunFinish:
// the event travels to its consumer, and its Args bytes must not alias
// the transcript's.
func snapshotPending(pending []ToolCallPart) []ToolCallPart {
	if pending == nil {
		return nil
	}
	out := make([]ToolCallPart, len(pending))
	for i, c := range pending {
		c.Args = cloneRaw(c.Args)
		out[i] = c
	}
	return out
}

// modelRequest is the copy of the step's request the model seam is
// handed (ModelRequest's godoc): its own Tools slice and its own
// Params values, so a model or middleware that reassigns a tool, or
// writes through a sampling knob's pointer, reaches neither what the
// step dispatches, what the request recorder remembers and re-emits
// for a retried attempt, nor the agent's or the caller's Params.
// Messages are already the model's own without a PrepareStep chain
// (requestMessages); the chain's result is copied again at the call
// site, since a function may return a message it keeps. One allocation
// for the slice, at most two more when knobs are set.
func modelRequest(req ModelRequest) ModelRequest {
	req.Tools = slices.Clone(req.Tools)
	req.Params = cloneParams(req.Params)
	return req
}

// cloneParams copies p's pointed-to values and its Stop slice, so a
// write through the copy's pointers never reaches p's: the pointer
// knobs share one fresh box (one allocation, none when all are nil)
// and Stop keeps its nil-ness. RequestParams holds no other reference
// (TestRequestParamsFieldsAreCopied pins it, and the fields of
// ModelRequest and Message the other copies — modelRequest,
// requestMessages, mw's attemptRequest — follow).
func cloneParams(p RequestParams) RequestParams {
	if p.Temperature != nil || p.TopP != nil || p.MaxTokens != nil || p.Seed != nil {
		box := new(struct {
			temperature, topP float64
			maxTokens         int
			seed              int64
		})
		if p.Temperature != nil {
			box.temperature = *p.Temperature
			p.Temperature = &box.temperature
		}
		if p.TopP != nil {
			box.topP = *p.TopP
			p.TopP = &box.topP
		}
		if p.MaxTokens != nil {
			box.maxTokens = *p.MaxTokens
			p.MaxTokens = &box.maxTokens
		}
		if p.Seed != nil {
			box.seed = *p.Seed
			p.Seed = &box.seed
		}
	}
	p.Stop = slices.Clone(p.Stop)
	return p
}

// requestMessages is the model seam's copy of the transcript: a fresh
// Messages slice and a fresh Content slice per message — every Content
// slice carved, capacity-capped, out of one shared backing array, so the
// copy costs two allocations whatever the transcript's length. A model
// or middleware that reassigns a message, or one of a message's parts,
// writes into its own copy and never into the run's transcript
// (ModelRequest's godoc). The part values themselves are shared: the
// bytes behind a tool call's Args or a file's Data stay read-only.
func requestMessages(msgs []Message) []Message {
	if msgs == nil {
		return nil
	}
	n := 0
	for _, m := range msgs {
		n += len(m.Content)
	}
	out := make([]Message, len(msgs))
	parts := make([]Part, n)
	for i, m := range msgs {
		if m.Content != nil {
			k := copy(parts, m.Content)
			m.Content, parts = parts[:k:k], parts[k:]
		}
		out[i] = m
	}
	return out
}

// cloneMessages deep-copies a transcript for a request handed to user
// code: each message and its parts are copied, so an in-place write —
// dropping a part, rewriting a tool call's raw arguments — cannot
// reach the run's transcript. Strings are immutable; the two mutable
// payloads, a tool call's Args and a file's Data, are re-allocated.
func cloneMessages(msgs []Message) []Message {
	out := make([]Message, len(msgs))
	for i, m := range msgs {
		content := make([]Part, len(m.Content))
		for j, p := range m.Content {
			switch p := p.(type) {
			case ToolCallPart:
				p.Args = cloneRaw(p.Args)
				content[j] = p
			case FilePart:
				p.Data = bytes.Clone(p.Data)
				content[j] = p
			default:
				content[j] = p
			}
		}
		m.Content = content
		out[i] = m
	}
	return out
}

// cloneTools deep-copies a tool list for the same boundary: each
// definition is a frozen-registry clone, so a PrepareStep function
// that rewrites a definition changes this step's snapshot only.
func cloneTools(tools []*ToolDef) []*ToolDef {
	out := make([]*ToolDef, len(tools))
	for i, t := range tools {
		out[i] = t.clone()
	}
	return out
}

// stopped reports whether any StopWhen condition is met and, when one
// is, the first in StopWhen order that fired, by its manifest name
// (stopName): the run's weft.run.stopped_by.
func (a *Agent) stopped(steps []StepRecord) (string, bool) {
	for _, cond := range a.stops {
		if cond.Stop(steps) {
			return stopName(cond), true
		}
	}
	return "", false
}

// loopState is the run-scoped state the guard carries between steps.
type loopState struct {
	// retries counts consecutive RETRY results per tool name; a
	// successful result for the tool resets it (Pydantic AI's rule: a
	// tool that eventually succeeds is not stuck).
	retries map[string]int
	// lastSig/streak track the loop detector: the previous step's tool
	// signature and how many consecutive steps have shared it.
	lastSig uint64
	streak  int
}

// guard is the continuation point: the checks that decide whether the
// loop may make another model call. The first breach wins; the others
// are not evaluated. The retry pass sorts its keys so an exhaustion
// error names one deterministic tool when several are stuck; the usage
// compares and the loop hash follow (ADR 0002: budgets are checked
// only when the loop would otherwise spend more).
func (a *Agent) guard(res *RunResult, rec *StepRecord, st *loopState) error {
	for _, name := range slices.Sorted(maps.Keys(st.retries)) {
		if st.retries[name] > a.maxModelRetries {
			return fmt.Errorf("%w: tool %q retried %d times, past MaxModelRetries(%d)",
				ErrModelRetriesExceeded, name, st.retries[name], a.maxModelRetries)
		}
	}
	if limit := a.usageLimit; limit.InputTokens > 0 || limit.OutputTokens > 0 {
		over := (limit.InputTokens > 0 && res.Usage.InputTokens > limit.InputTokens) ||
			(limit.OutputTokens > 0 && res.Usage.OutputTokens > limit.OutputTokens)
		if over {
			return fmt.Errorf("%w (input %d/%d, output %d/%d)",
				ErrUsageLimit, res.Usage.InputTokens, limit.InputTokens,
				res.Usage.OutputTokens, limit.OutputTokens)
		}
	}
	if a.detectLoops > 0 {
		st.advance(stepSignature(rec.ToolCalls))
		if st.streak >= a.detectLoops {
			return fmt.Errorf("%w: %d identical steps in a row", ErrLoopDetected, st.streak)
		}
	}
	return nil
}

// advance moves the loop detector past one step with signature sig.
func (st *loopState) advance(sig uint64) {
	if sig == st.lastSig && st.streak > 0 {
		st.streak++
	} else {
		st.streak = 1
	}
	st.lastSig = sig
}

// keptCalls is the tool calls of the last n assistant messages of msgs,
// oldest first — the kept steps KeptSteps names (one assistant message
// per step). Fewer when msgs holds fewer.
func keptCalls(msgs []Message, n int) [][]ToolCallPart {
	var out [][]ToolCallPart
	for i := len(msgs) - 1; i >= 0 && len(out) < n; i-- {
		if msgs[i].Role != RoleAssistant {
			continue
		}
		var calls []ToolCallPart
		for _, p := range msgs[i].Content {
			if c, ok := p.(ToolCallPart); ok {
				calls = append(calls, c)
			}
		}
		out = append(out, calls)
	}
	slices.Reverse(out)
	return out
}

// stepSignature hashes a step's tool calls — name and raw argument
// bytes — sorted, so a permuted batch is the same request ("the same
// set" in DetectLoops' spec is literal). The args are hashed as the
// model emitted them, without canonicalisation: a model that reformats
// its JSON has changed something, and the cost of a false negative is
// one more step. Results are deliberately absent (ADR 0002).
func stepSignature(calls []ToolCallPart) uint64 {
	pairs := make([][2]string, len(calls))
	for i, c := range calls {
		pairs[i] = [2]string{c.Name, string(c.Args)}
	}
	slices.SortFunc(pairs, func(a, b [2]string) int {
		if c := strings.Compare(a[0], b[0]); c != 0 {
			return c
		}
		return strings.Compare(a[1], b[1])
	})
	h := fnv.New64a()
	for _, p := range pairs {
		h.Write([]byte(p[0]))
		h.Write([]byte{0})
		h.Write([]byte(p[1]))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// modelInfo reports the model's identity when it implements the
// optional Info interface; the zero ModelInfo otherwise.
func (a *Agent) modelInfo() ModelInfo { return InfoOf(a.model) }

// execTools runs one step's tool calls with bounded concurrency, all of
// them resolved against tools — the step's snapshot: advertising showed
// that list, so dispatch cannot name-resolve against anything fresher.
// Results are returned in call order regardless of completion order —
// determinism for the transcript; ToolStart/ToolFinish events carry the
// live interleaving. A failing tool never cancels its siblings; only
// ctx does.
//
// The event order rule (TestToolEventOrderRule): ToolStarts in call
// order, each ToolFinish after its own ToolStart, ToolFinishes in
// completion order. The batch is not fenced — call i's goroutine may
// emit its ToolFinish before the dispatcher emits call i+1's ToolStart —
// and must not be: a start waiting on a slot or a barrier can only
// follow a finish, and a start emitted before its call holds a slot
// would claim a call had started that had not.
//
// The concurrency slot is acquired here, in call order, before the tool's
// goroutine is spawned. That is what makes "in call order" true: tools
// start in the order the model requested them, and under Sequential each
// one finishes before the next begins. Calls that never obtain a slot
// because ctx was canceled produce an error result and no events.
//
// A tool marked Sequential is a barrier: the dispatcher waits for the
// in-flight calls to finish, runs it alone, and resumes. Calls the chain
// parks with ErrApprovalRequired are returned as pending rather than as
// results: they had a ToolStart and get no ToolFinish. approved is the
// run's decisions when the calls are resumed under them (nil for a
// step's own calls): an approved call runs with Call.Approved set, and
// its provenance (ApproveBy) rides its execute_tool span.
//
// Every dispatched call carries a nest: how a Subagent handler reports
// its child run's events (wrapped in Nested, numbered from this run's
// counter under emitMu) and usage (the returned per-call map, keyed by
// call id). The dispatcher otherwise knows nothing about subagents.
//
// rec is the run's recorder, told each call's truncation before its
// ToolFinish is emitted (the tool_finish record's
// weft.tool.truncated_bytes); nil records nothing.
func (a *Agent) execTools(ctx context.Context, runID string, step int, tools []*ToolDef, calls []ToolCallPart, seq *atomic.Int64, emit func(Event), approved map[string]decision, parallelism int, parkSet *parkRule, rec *recorder, retryBase map[string]int, keys func(Call) string, dispatch DispatchFunc, after AfterDispatchFunc, parkUnknown bool, only []string) (results []ToolResultPart, pending []ToolCallPart, subagents map[string]Usage, retried []string) {
	outcomes := make([]ToolResultPart, len(calls))
	parked := make([]bool, len(calls))
	sem := make(chan struct{}, parallelism)
	subs := map[string]Usage{}
	// A counted RETRY's span waits for its mark (below): retriedAt[i]
	// marks call i's outcome a RETRY, endRetry[i] ends its span.
	retriedAt := make([]bool, len(calls))
	endRetry := make([]func(retryMark), len(calls))

	// Event-ordering rule for concurrent tools: the Seq is assigned and the
	// event emitted under one lock, so observed order always matches Seq
	// order. Without this, two tools could emit events whose arrival order
	// contradicts their sequence numbers — unreplayable streams.
	var emitMu sync.Mutex
	ordered := func(ev func(seq int64) Event) {
		emitMu.Lock()
		defer emitMu.Unlock()
		emit(ev(seq.Add(1)))
	}
	// nestFor builds one call's reporting channel. emit and usage both
	// check the closed flag and act under emitMu, so a child event is
	// either fully before or fully after the call's ToolFinish, and a
	// usage record either lands in the map the step reports or is
	// dropped with the abandoned goroutine that produced it — a timed
	// out or cancelled delegation is not waited for, and its result was
	// already recorded (the usage half of the late-event rule, ADR 0004
	// and ADR 0014).
	nestFor := func(call ToolCallPart) *nest {
		n := &nest{callID: call.ID}
		n.emit = func(ev Event) {
			emitMu.Lock()
			defer emitMu.Unlock()
			if n.closed.Load() {
				return
			}
			emit(Nested{RunID: runID, Seq: seq.Add(1), CallID: call.ID, Event: ev})
		}
		n.usage = func(u Usage) {
			emitMu.Lock()
			defer emitMu.Unlock()
			if n.closed.Load() {
				return
			}
			subs[call.ID] = subs[call.ID].Add(u)
		}
		return n
	}

	var wg sync.WaitGroup
	var inflight atomic.Int64 // calls started and not yet returned
	for i, call := range calls {
		def, _ := findTool(tools, call.Name)
		barrier := def != nil && def.sequential
		// The call's queue wait (weft.tool.queue_ms): measured only when
		// it had to wait — behind a barrier with calls before it, or for
		// a Parallelism slot — so a call that started at once has none.
		var queued time.Time
		if barrier {
			// Only a Wait that blocks is a wait: a barrier call with
			// nothing in flight (first in the batch, or after another
			// barrier) starts at once.
			if inflight.Load() > 0 {
				queued = time.Now()
			}
			// Let everything in flight finish before this call starts.
			// wg.Add only happens on this goroutine, so Wait is safe.
			wg.Wait()
		}
		// Never start a tool once the run is canceled. The explicit check
		// makes this deterministic: a select alone picks at random when a
		// slot frees up at the same moment ctx is done.
		canceled := ctx.Err() != nil
		if !canceled {
			select {
			case sem <- struct{}{}:
			default:
				if queued.IsZero() {
					queued = time.Now()
				}
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					canceled = true
				}
			}
		}
		var queue time.Duration
		if !queued.IsZero() {
			queue = sinceAtLeast(queued)
		}
		if canceled {
			outcomes[i] = ToolResultPart{
				CallID:  call.ID,
				Name:    call.Name,
				IsError: true,
				Content: "run canceled before the tool started",
			}
			continue
		}
		// ToolStart is emitted here, on the dispatching goroutine, so
		// start events are in call order by construction. The Seq is
		// captured for the tool span's weft.tool.seq attribute; the
		// goroutine launch happens after this emit, so the read is safe.
		var startSeq int64
		ordered(func(s int64) Event {
			startSeq = s
			return ToolStart{RunID: runID, Seq: s, CallID: call.ID, Name: call.Name, Args: cloneRaw(call.Args)}
		})
		prov, ok := approved[call.ID]
		isApproved := ok && prov.approved
		c := Call{RunID: runID, Step: step, CallID: call.ID, Name: call.Name, Approved: isApproved}
		var keyErr error
		if keys != nil {
			c.IdempotencyKey, keyErr = a.safeKey(ctx, keys, c)
		}
		if c.IdempotencyKey == "" {
			c.IdempotencyKey = defaultIdempotencyKey(runID, call.ID)
		}
		// An approved call on edited arguments (ApproveEdited): its result
		// starts with the pinned EDITED line, under the result cap.
		var edited string
		if isApproved && prov.args != nil {
			edited = editedPrefix + string(prov.args) + "\n"
		}
		wg.Add(1)
		inflight.Add(1)
		go func() {
			defer wg.Done()
			defer inflight.Add(-1)
			defer func() { <-sem }()
			n := nestFor(call)
			callCtx := withNest(withCall(ctx, c), n)
			if dispatch != nil {
				callCtx = withDispatch(callCtx, dispatch)
			}
			if after != nil {
				callCtx = withAfterDispatch(callCtx, after)
			}
			if parkUnknown {
				callCtx = withParkUnknown(callCtx)
			}
			if only != nil {
				callCtx = withOnlyTools(callCtx, only)
			}
			// A CallTool from a tool middleware, before the call reaches
			// its handler, is keyed (or, under a gate, refused) from this
			// scope; the chain's base installs the handler's own.
			callCtx = withInner(callCtx, &innerDispatch{outer: c, gate: dispatch, after: after, early: true})
			// A CallTool from inside the handler dispatches under the
			// same park rule as the loop (WR-7): approval never fails open.
			callCtx = withParkRule(callCtx, parkSet)
			// The tool call's own reporting: one execute_tool span, on the
			// handler's context — a span the handler starts is its child,
			// and a child run's invoke_agent hangs under it — ended with
			// the call's outcome, before ToolFinish is emitted so no span
			// operation runs under emitMu (ADR 0016).
			callCtx, endTool := a.obs.tool(callCtx, rec, c, startSeq, toolStartFacts{timeout: a.callTimeout(def), queue: queue, contentOff: a.content != nil && !*a.content,
				approvedBy: prov.by, grantID: prov.grant, argsRepaired: rec.takeRepaired(call.ID), origin: def.originName()})
			var retry bool
			var fail toolFailure
			var cut int
			var park string
			if keyErr != nil {
				// The call's key could not be named: it never starts
				// (fail closed), and the model reads the refusal.
				fail = readToolError(keyErr)
				outcomes[i] = ToolResultPart{CallID: call.ID, Name: call.Name, IsError: true}
				outcomes[i].Content, cut = capResultN(fail.text, a.callCap(def))
			} else {
				outcomes[i], park, retry, cut, fail = a.callTool(callCtx, call, def, parkSet, rec, edited)
			}
			parked[i] = park != ""
			retriedAt[i] = retry
			if retry && retryBase != nil {
				// Its mark depends on the whole batch: the span ends
				// after wg.Wait, stamped now. Attribution only — the
				// ToolFinish below is not delayed.
				at := time.Now()
				res, c := outcomes[i], cut
				endRetry[i] = func(m retryMark) { endTool(res, "", fail, c, m, at) }
			} else {
				endTool(outcomes[i], park, fail, cut, retryMark{}, time.Time{})
			}
			if parked[i] {
				// A parked call has no ToolFinish to close its nest
				// under, and its handler may still be running (an
				// unknown outcome parked under ParkUnknown, a layer
				// that gave up on a child): close it here, under the
				// same lock, so nothing it emits or records later
				// reaches a step the loop has already returned.
				emitMu.Lock()
				n.closed.Store(true)
				emitMu.Unlock()
				return
			}
			if cut > 0 {
				rec.noteTruncated(call.ID, cut)
			}
			ordered(func(s int64) Event {
				// The late-event rule (ADR 0004): the close rides under
				// emitMu with the finish, so no Nested event for this
				// call can follow its ToolFinish in the stream.
				n.closed.Store(true)
				return ToolFinish{
					RunID:   runID,
					Seq:     s,
					CallID:  call.ID,
					Name:    call.Name,
					Content: outcomes[i].Content,
					IsError: outcomes[i].IsError,
				}
			})
		}()
		if barrier {
			// Run alone: nothing after it starts until it is done.
			wg.Wait()
		}
	}
	wg.Wait()
	// The retry marks (weft.tool.retry), assigned once the batch is
	// known, in call order — the guard's count exactly: the steps
	// before plus this batch's RETRYs of the tool, none when the same
	// batch has a success of that tool (the guard resets it).
	succeeded := map[string]bool{}
	for i, call := range calls {
		if !parked[i] && !outcomes[i].IsError {
			succeeded[call.Name] = true
		}
	}
	counted := map[string]int{}
	for i, call := range calls {
		if !retriedAt[i] {
			continue
		}
		retried = append(retried, call.Name)
		if endRetry[i] == nil {
			continue
		}
		var mark retryMark
		if !succeeded[call.Name] {
			counted[call.Name]++
			mark = retryMark{n: retryBase[call.Name] + counted[call.Name], budget: a.maxModelRetries}
		}
		endRetry[i](mark)
	}
	results = make([]ToolResultPart, 0, len(calls))
	for i, call := range calls {
		if parked[i] {
			pending = append(pending, call)
			continue
		}
		results = append(results, outcomes[i])
	}
	return results, pending, subs, retried
}

// resolvePending runs the calls an earlier run left pending, under this
// run's decisions: approved calls execute through the ordinary chain
// with Call.Approved set; denied and undecided calls become error
// results the model sees. Results come back in call order; a call the
// chain parks again is returned as pending. The step index of the
// resumed calls is reported as 0: the original index is not recoverable
// from the transcript, and there is no StepRecord for them (ADR 0007).
// Audit lines should key on the CallID, not the step.
func (a *Agent) resolvePending(ctx context.Context, cfg runConfig, calls []ToolCallPart, seq *atomic.Int64, emit func(Event), fetchTools func() ([]*ToolDef, error), parkSet *parkRule, rec *recorder) ([]ToolResultPart, []ToolCallPart, map[string]Usage, error) {
	// Resumed calls run before any step exists, so they fetch their own
	// snapshot — a separate consultation, like CallTool's, through the
	// run's (possibly narrowed) fetch.
	tools, err := fetchTools()
	if err != nil {
		return nil, nil, nil, err
	}
	var approved []ToolCallPart
	for _, c := range calls {
		if d, ok := cfg.decisions[c.ID]; ok && d.approved {
			if d.args != nil {
				c.Args = cloneRaw(d.args) // the decider's arguments, executed as given
			}
			approved = append(approved, c)
		}
	}
	ran, pending, sub, _ := a.execTools(ctx, cfg.id, 0, tools, approved, seq, emit, cfg.decisions, cfg.effectiveParallelism(a.parallelism), parkSet, rec, nil, cfg.keys, cfg.dispatch, cfg.afterDispatch, cfg.parkUnknown, cfg.onlyTools)
	byID := make(map[string]ToolResultPart, len(ran))
	for _, r := range ran {
		byID[r.CallID] = r
	}
	parked := make(map[string]bool, len(pending))
	for _, c := range pending {
		parked[c.ID] = true
	}
	// A call that parks again is reported as the transcript holds it —
	// the model's own arguments, never an edit's (C4): the next run
	// resumes the transcript's call, so a plain Approve of what Pending
	// shows runs exactly what it shows. An edit covers one resume; to
	// run it again, decide it again with ApproveEdited.
	pending = pending[:0]
	for _, c := range calls {
		if parked[c.ID] {
			pending = append(pending, c)
		}
	}
	results := make([]ToolResultPart, 0, len(calls))
	for _, c := range calls {
		if parked[c.ID] {
			continue
		}
		// A resolved call never runs: the decision's content is the
		// result, verbatim, capped as any result would be (the cap is
		// a transcript rule, not an execution rule) — a result in the
		// tool message, no span, no events, Call.Approved never set
		// (ADR 0007's 2026-09-22 amendment); weft.resume.calls names
		// it resolved.
		if d, ok := cfg.decisions[c.ID]; ok && d.resolved {
			resultCap := a.resultCap
			if def, _ := findTool(tools, c.Name); def != nil && def.capSet {
				resultCap = def.resultCap
			}
			results = append(results, ToolResultPart{
				CallID:  c.ID,
				Name:    c.Name,
				IsError: d.isError,
				Content: capResult(d.content, resultCap),
			})
			continue
		}
		if r, ok := byID[c.ID]; ok {
			// An edited call's result already starts with the EDITED
			// line (execTools), under the result cap.
			results = append(results, r)
			continue
		}
		reason, kind, by, grant := "no decision", deniedKindNoDecision, deniedByApproval, ""
		if d, ok := cfg.decisions[c.ID]; ok {
			reason, kind = d.reason, deniedKindDenied
			if d.by == deniedByGrant {
				by, grant = deniedByGrant, d.grant
			}
		}
		results = append(results, ToolResultPart{
			CallID:  c.ID,
			Name:    c.Name,
			IsError: true,
			Content: deniedResult(reason),
		})
		// A denied call never ran: no event (ADR 0007), a zero-length
		// execute_tool span naming its kind (UX-E-17), and the run's
		// root counts it (weft.run.denied).
		a.obs.deniedTool(ctx, rec, cfg.id, c, kind, by, grant)
		rec.noteDenied()
	}
	return results, pending, sub, nil
}

// maxResumeCalls bounds weft.resume.calls: the first 32 parked calls
// are listed, the rest counted (",…+k"), so one wide boundary cannot
// grow the run's span and run_start record without bound.
const maxResumeCalls = 32

// resumeCallsAttr renders weft.resume.calls: each pending call of a
// resume, in call order, as <call id>=<kind> joined by commas — kind
// approved (Approve), denied (Deny), resolved (Resolve, ResolveError)
// or no_decision (left undecided: rule 12's "DENIED: no decision").
// "" when the run resumes no pending call. Ids and enums only, never a
// reason or a resolved payload (ADR 0016). The kind is the decision,
// not the outcome: an approved call may still fail or park again.
// Capped at maxResumeCalls entries: past it the value ends ",…+k", k
// the calls not listed.
func resumeCallsAttr(calls []ToolCallPart, decisions map[string]decision) string {
	if len(calls) == 0 {
		return ""
	}
	var b strings.Builder
	for i, c := range calls {
		if i > 0 {
			b.WriteByte(',')
		}
		if i == maxResumeCalls {
			b.WriteString("…+")
			b.WriteString(strconv.Itoa(len(calls) - i))
			break
		}
		kind := deniedKindNoDecision
		if d, ok := decisions[c.ID]; ok {
			switch {
			case d.approved:
				kind = "approved"
			case d.resolved:
				kind = "resolved"
			default:
				kind = deniedKindDenied
			}
		}
		b.WriteString(c.ID)
		b.WriteByte('=')
		b.WriteString(kind)
	}
	return b.String()
}

// unresolvedCalls returns the tool calls of the last assistant message
// that have no result on the tool message directly after it — the
// calls an earlier run left pending.
func unresolvedCalls(msgs []Message) []ToolCallPart {
	i := lastAssistantWithCalls(msgs)
	if i < 0 {
		return nil
	}
	served := map[string]bool{}
	if i+1 < len(msgs) && msgs[i+1].Role == RoleTool {
		for _, p := range msgs[i+1].Content {
			if r, ok := p.(ToolResultPart); ok {
				served[r.CallID] = true
			}
		}
	}
	var out []ToolCallPart
	for _, p := range msgs[i].Content {
		if c, ok := p.(ToolCallPart); ok && !served[c.ID] {
			out = append(out, c)
		}
	}
	return out
}

func lastAssistantWithCalls(msgs []Message) int {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role != RoleAssistant {
			continue
		}
		for _, p := range msgs[i].Content {
			if _, ok := p.(ToolCallPart); ok {
				return i
			}
		}
		return -1
	}
	return -1
}

// attachResults places results on the tool message directly after the
// assistant message that issued calls, creating that message when the
// earlier run recorded none — so the transcript's shape is the
// canonical one whether or not other tools ran in that step. The
// message is rebuilt in the assistant's call order (ADR 0007 §3), each
// call taking its resumed result when one exists and its earlier one
// otherwise: Gemini matches functionResponses by name and position, so
// a reordered tool message could attach a result to the wrong call.
// joined returns the completed tool message either way — created, or
// rebuilt over the partial one the earlier run left (ADR 0024 S1.3
// widened the report to the rebuild): the caller hands it to the
// transcript observers and the messages record. nil when no result was
// attached.
func attachResults(msgs []Message, calls []ToolCallPart, results []ToolResultPart) (out []Message, joined []Message) {
	if len(results) == 0 {
		return msgs, nil
	}
	i := lastAssistantWithCalls(msgs)
	if i < 0 {
		return msgs, nil
	}
	resumed := make(map[string]ToolResultPart, len(results))
	for _, r := range results {
		resumed[r.CallID] = r
	}
	existing := map[string][]ToolResultPart{}
	if i+1 < len(msgs) && msgs[i+1].Role == RoleTool {
		for _, p := range msgs[i+1].Content {
			if r, ok := p.(ToolResultPart); ok {
				existing[r.CallID] = append(existing[r.CallID], r)
			}
		}
	}
	parts := make([]Part, 0, len(resumed)+len(existing))
	for _, p := range msgs[i].Content {
		c, ok := p.(ToolCallPart)
		if !ok {
			continue
		}
		if r, ok := resumed[c.ID]; ok {
			parts = append(parts, r)
			continue
		}
		if rs := existing[c.ID]; len(rs) > 0 {
			parts = append(parts, rs[0])
			existing[c.ID] = rs[1:]
		}
	}
	if i+1 < len(msgs) && msgs[i+1].Role == RoleTool {
		out := slices.Clone(msgs)
		out[i+1].Content = parts
		return out, []Message{out[i+1]}
	}
	created := Message{Role: RoleTool, Content: parts}
	return slices.Insert(slices.Clone(msgs), i+1, created), []Message{created}
}

// composeSystem appends the advertised tools' PromptSnippets to the
// agent's instructions: one paragraph per tool, in order, blank-line
// separated. Without snippets the instructions pass through unchanged.
func composeSystem(system string, tools []*ToolDef) string {
	var b strings.Builder
	b.WriteString(system)
	for _, t := range tools {
		if t.snippet == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(t.snippet)
	}
	return b.String()
}

// Model-visible result strings — contract, pinned by tests (ADR 0002,
// ADR 0007).
func truncatedCallResult(name string) string {
	return fmt.Sprintf("tool call %s was not executed: the response hit the output token limit", name)
}

func deniedResult(reason string) string {
	return (&ToolError{Code: CodeDenied, Message: reason, Err: ErrApprovalDenied}).Error()
}

// CallTool dispatches one tool call by name the way the loop does —
// through the agent's WrapTools chain and the tool's own — and returns
// the result text the model would see. It is the seam for manual
// dispatchers. Unlike the loop it does not contain failures or apply
// run policy: an unknown name returns an error wrapping ErrNoSuchTool,
// undecodable arguments one wrapping ErrInvalidToolInput, a
// RequireApproval tool one wrapping ErrApprovalRequired, and handler
// errors, middleware errors, and panics propagate. A tool source whose
// snapshot carries a duplicate name returns an error wrapping
// ErrDuplicateTool. Timeouts and result caps are not applied;
// StrictInput is, at both levels, as in the loop. No span or log line
// is produced: manual dispatchers own their context and their own
// reporting (ADR 0016). Outside a run it therefore leaves no trace in
// the record at all — no run, no event, no span — so a trace
// reader has nothing to show for it. Called from a tool handler, the dispatched call
// is its own: CallFromContext in its handler reports its id and name
// (the run and step stay the dispatching call's), and an approval the
// dispatching call ran under does not carry over — a RequireApproval
// tool still returns ErrApprovalRequired, and so does a tool the run's
// park rule names (ParkOn, ParkAllExcept, an agent ParkSource): the run
// parks it for the loop, and it parks it for the handler too. That park
// is the handler's to read, not its call's: returned by the handler
// (wrapped or not), it fails the dispatching call — the model reads
// that the inner tool requires approval and did not run — instead of
// parking it, for an approval of the dispatching call would not carry
// to the inner one and its rerun would park again. It is a
// call with effects like the loop's own: it carries an IdempotencyKey
// derived from the dispatching call's key and its position among that
// handler's CallTool calls (the same on a retry that dispatches in the
// same order), passes the run's BeforeDispatch gate (Call.Outer names
// the dispatching call; a refusal is an error wrapping
// ErrNotDispatched), and a handler of a tool that is not ReplaySafe
// cut off by the run's cancellation before it answered returns an
// error wrapping ErrOutcomeUnknown — return it, and the dispatching
// call's outcome is unknown too. Inner calls start only while their
// outer call is open: once the loop has settled the dispatching call —
// answered it as timed out or OUTCOME_UNKNOWN at its Timeout or the
// run's cancellation, or read its handler's return — a CallTool from
// that handler (still running past its deadline, or a goroutine it
// left behind) is refused with an error wrapping ErrNotDispatched and
// the inner handler never runs (since 0.24.1), so no effect starts
// behind an answer the model has already read. It resolves against the run's tools:
// a tool the run's OnlyTools narrowed away is unknown (ErrNoSuchTool).
// Called from a WrapTools middleware of a loop call — before that
// call's handler has started — it is not a handler's inner call: under
// a BeforeDispatch gate it is refused with an error wrapping
// ErrNotDispatched (nothing has let an effect start under the outer
// call yet), and with none it runs keyed from the outer call's key
// apart from the handler's (Call.IdempotencyKey). A run started on a
// call's context — a Subagent's child — inherits none of this: its
// calls answer to its own options.
func (a *Agent) CallTool(ctx context.Context, call ToolCallPart) (string, error) {
	tools, err := a.dispatchTools()
	if err != nil {
		return "", err
	}
	if only := onlyToolsFrom(ctx); only != nil {
		// From inside a run narrowed by OnlyTools: the inner call
		// resolves against the run's tools, as the loop's own calls do —
		// a narrowed-away tool is unknown (ErrNoSuchTool).
		tools = narrowTools(tools, only)
	}
	def, _ := findTool(tools, call.Name)
	strict := a.strict
	if def != nil && def.strict {
		strict = true
	}
	// Dispatched from inside a tool handler, ctx carries that handler's
	// Call — possibly approved. The dispatched call is a call of its
	// own: never approved (an approval names one call, not what its
	// handler goes on to do), and its handler sees its own id and name,
	// in the same run and step. Outside the loop there is no Call, and
	// none is invented.
	outer, nested := CallFromContext(ctx)
	if nested {
		inner := Call{RunID: outer.RunID, Step: outer.Step, CallID: call.ID, Name: call.Name, Outer: outer.CallID}
		// The handler's own scope matches the whole Call — run, step and
		// id — never the id alone: a child run's call may reuse its
		// parent's call id (Gemini's call_0).
		if in := innerFrom(ctx); in != nil && !in.early && in.outer == outer {
			if in.mark != nil && in.mark.wasSettled() {
				// The loop has already answered the outer call (its
				// Timeout, the run's cancellation, or its return): no
				// effect starts behind an answer the model has read.
				return "", fmt.Errorf("%w: tool %q was dispatched by the handler of call %q after the loop had settled that call (it timed out, was canceled, or returned); a call starts only while its outer call is open", ErrNotDispatched, call.Name, outer.CallID)
			}
			// Keyed from the outer call and its position, and gated like
			// the loop's own calls: a retry of the outer call that
			// dispatches in the same order reuses the same keys.
			// An outer call with no key (a served request whose client
			// sent none) gives its inner calls none: a key derived from
			// "" would be one constant shared by every keyless request.
			if n := in.n.Add(1); in.outer.IdempotencyKey != "" {
				inner.IdempotencyKey = innerKey(in.outer.IdempotencyKey, n)
			}
			if in.gate != nil {
				ctx = withDispatch(ctx, in.gate)
			}
			if in.after != nil {
				ctx = withAfterDispatch(ctx, in.after)
			}
		} else if in != nil && in.early && in.outer == outer {
			// From a WrapTools middleware: the outer call has not passed
			// the gate, its handler has not started, so nothing has let
			// an effect begin under it. Under a gate the inner call is
			// refused (ADR 0031 §4's 2026-11-05 amendment: inner calls
			// start only under a started outer call); with none it is
			// keyed from the outer key and its position among the
			// middleware's CallTool calls, apart from the handler's.
			if in.gate != nil {
				return "", fmt.Errorf("%w: tool %q was dispatched from a tool middleware of call %q, before that call's handler started; under a dispatch gate an inner call is dispatched only from inside the handler", ErrNotDispatched, call.Name, outer.CallID)
			}
			if n := in.n.Add(1); in.outer.IdempotencyKey != "" {
				inner.IdempotencyKey = middlewareKey(in.outer.IdempotencyKey, n)
			}
		}
		ctx = withCall(ctx, inner)
	}
	// The run's park rule (ParkOn, ParkAllExcept, ParkSource) holds for
	// a call dispatched from inside a handler as it does for the loop's
	// own: a parked inner call returns an error wrapping
	// ErrApprovalRequired to the handler, never runs unasked (WR-7).
	mark := new(handlerMark)
	out, err := a.chain(def, strict, parkRuleFrom(ctx), mark, nil)(ctx, call)
	// Settled for the handler's own CallTool calls too: one it leaves
	// running dispatches nothing after this answer.
	mark.abandon()
	if err != nil && def != nil && def.ReplayPolicy() != ReplaySafe && mark.wasEntered() &&
		ctx.Err() != nil && !errIsContained(err, ErrOutcomeUnknown) {
		// The handler started and the run was cut off under it before
		// it answered: its effect is unknown, never a plain failure.
		err = fmt.Errorf("%w: tool %q was cut off before it answered: %w", ErrOutcomeUnknown, call.Name, err)
	}
	if err != nil && nested && errIsContained(err, ErrApprovalRequired) {
		// Parked for the handler: it reads ErrApprovalRequired. Returned
		// by the handler, it fails the outer call rather than parking
		// it — an approval names the outer call only, never the calls
		// its handler dispatches, so a rerun would park again for ever.
		err = &dispatchedParkError{tool: call.Name, err: err}
	}
	return out, err
}

// dispatchedParkError is the park of a call a handler dispatched with
// CallTool: it matches ErrApprovalRequired for the handler, and the
// loop reads it as the handler's failure, never as its own call's park
// (readToolError). Its text is model-visible (pinned bytes).
type dispatchedParkError struct {
	tool string
	err  error
}

func (e *dispatchedParkError) Error() string {
	return fmt.Sprintf("tool %q, dispatched by this call's handler, requires approval and did not run: a dispatched call is never parked for one", e.tool)
}

func (e *dispatchedParkError) Unwrap() error { return e.err }

type parkRuleKey struct{}

func withParkRule(ctx context.Context, p *parkRule) context.Context {
	if p == nil {
		return ctx
	}
	return context.WithValue(ctx, parkRuleKey{}, p)
}

func parkRuleFrom(ctx context.Context) *parkRule {
	p, _ := ctx.Value(parkRuleKey{}).(*parkRule)
	return p
}

// chain builds the tool-call chain for one call: agent middleware
// (outermost, first registered first) around the tool's middleware
// around the base caller, which resolves the tool, enforces
// RequireApproval and the park rule, passes the dispatch gate, and
// runs the handler — through the tool's handler middleware
// (WrapHandler), which decodes nothing itself. def is nil for an
// unknown tool — the chain still runs, so Allow/Audit see the attempt,
// and the base returns the NO_SUCH_TOOL error.
//
// mark, when not nil, notes a panic out of the handler itself as it
// unwinds through the base (handlerMark): the containment then tells a
// handler's panic from one the middleware around it raised (the
// observer's weft.panic.layer; nothing else reads it).
//
// layers, when not nil (the development detail level), puts a "layer
// <name>" span outside each middleware, numbered outer to inner across
// the agent's and the tool's lists.
func (a *Agent) chain(def *ToolDef, strict bool, parkSet *parkRule, mark *handlerMark, layers *toolLayerSpans) ToolCaller {
	base := func(ctx context.Context, call ToolCallPart) (string, error) {
		if def == nil {
			return "", noSuchTool(call.Name)
		}
		// The approval boundary (ADR 0007): the tool's own RequireApproval,
		// or the run's park rule naming it (ParkOn, ParkAllExcept). Same
		// boundary, same resume path — the rule is the policy applied per
		// run (ADR 0024 D7).
		if def.approval || parkSet.parks(def.Name, a.hasOutput && def.Name == outputToolName) {
			if c, _ := CallFromContext(ctx); !c.Approved {
				if mark != nil {
					by := parkByApproval
					if !def.approval {
						by = parkSet.parkedBy(def.Name, a.hasOutput && def.Name == outputToolName)
					}
					mark.park(by)
				}
				return "", fmt.Errorf("%w: tool %q", ErrApprovalRequired, call.Name)
			}
		}
		// The dispatch gate (BeforeDispatch): the last word before the
		// handler runs, past every middleware and the approval
		// boundary. It is cleared from the handler's context, so a
		// child run never inherits it; a CallTool inside the handler
		// passes it again, as an inner call (innerDispatch).
		gate := dispatchFrom(ctx)
		c, inLoop := CallFromContext(ctx)
		if gate != nil {
			ctx = withDispatch(ctx, nil)
			if err := gate(ctx, c, def); err != nil {
				return "", fmt.Errorf("%w: %w", ErrNotDispatched, err)
			}
		}
		// AfterDispatch: told when the handler that starts below has
		// returned (or panicked) — also when the loop has abandoned it
		// at its Timeout or the run's cancellation, for this goroutine
		// carries on to the end.
		after := afterDispatchFrom(ctx)
		if after != nil {
			ctx = withAfterDispatch(ctx, nil)
			defer after(ctx, c, def)
		}
		if inLoop {
			// A CallTool from inside the handler dispatches under this
			// call: a key derived from its key and the inner call's
			// position, through the same gate (P1-5).
			ctx = withInner(ctx, &innerDispatch{outer: c, gate: gate, after: after, mark: mark})
		}
		// The handler, with its own middleware (WrapHandler) around it:
		// past the approval boundary and the gate, inside AfterDispatch.
		// Built only for a tool that has some: the plain path allocates
		// nothing more.
		var handler ToolCaller
		if len(def.hmw) > 0 {
			handler = func(ctx context.Context, call ToolCallPart) (string, error) {
				return def.invoke(ctx, call.Args, strict)
			}
			for i := len(def.hmw) - 1; i >= 0; i-- {
				handler = def.hmw[i](handler)
			}
		}
		if mark == nil {
			if handler != nil {
				return handler(ctx, call)
			}
			return def.invoke(ctx, call.Args, strict)
		}
		// Every return path leaves the mark right: a normal return
		// notes the error the handler returned (the layer a panic in
		// its methods is attributed to, settleToolError); a panic is
		// noted with its value and goes on unwinding unchanged (the
		// same value re-raised), so a middleware that recovers it sees
		// what the handler threw.
		defer func() {
			if p := recover(); p != nil {
				mark.note(p)
				panic(p)
			}
		}()
		if !mark.enter() {
			// The loop gave up on the call (its Timeout passed, or the
			// run was canceled) while it was still on its way here — in
			// a slow gate, say — and answered it: it never starts now,
			// or the model would have been told a plain failure for an
			// effect that happened.
			return "", fmt.Errorf("%w: tool %q: the loop gave up on the call before its handler started", ErrNotDispatched, call.Name)
		}
		var out string
		var err error
		if handler != nil {
			out, err = handler(ctx, call)
		} else {
			out, err = def.invoke(ctx, call.Args, strict)
		}
		mark.returning(err)
		return out, err
	}
	c := base
	if def != nil {
		for i := len(def.mw) - 1; i >= 0; i-- {
			c = def.mw[i](c)
			if layers != nil {
				c = layers.wrap(c, funcInfo(def.mw[i]).Name, len(a.toolMW)+i)
			}
		}
	}
	for i := len(a.toolMW) - 1; i >= 0; i-- {
		c = a.toolMW[i](c)
		if layers != nil {
			c = layers.wrap(c, funcInfo(a.toolMW[i]).Name, i)
		}
	}
	return c
}

// callTool dispatches one call under the tool's effective policy and
// folds every failure — unknown tool, bad arguments, handler error,
// handler or middleware panic, timeout — into an error result the model
// can correct: a tool failure is data, not a run failure. def is the
// call's resolution against the step's (or CallTool's) snapshot; nil
// for an unknown name. Per-tool options override the agent's defaults;
// the result cap is applied last, over every outcome. An error wrapping
// ErrApprovalRequired is the one non-result: the call is reported
// pending instead. The chain's error is returned beside the result —
// folded into the result's text for the model, but typed for the
// observer's error.type (ADR 0016) — and is zero on success and on a
// parked call's pending report. Every fact of it the loop and the
// observer read is read once, contained (settleToolError): a panic in
// the error's own methods is the tool's failure too.
//
// cut is the bytes the result cap removed from the content (0 when it
// cut nothing): the observer's weft.tool.truncated_bytes.
func (a *Agent) callTool(ctx context.Context, call ToolCallPart, def *ToolDef, parkSet *parkRule, rec *recorder, edited string) (res ToolResultPart, park string, retry bool, cut int, fail toolFailure) {
	res = ToolResultPart{CallID: call.ID, Name: call.Name}
	resultCap := a.callCap(def)
	timeout := a.toolTimeout
	strict := a.strict
	if def != nil {
		if def.timeoutSet {
			timeout = def.timeout // includes the Timeout(0) lift
		}
		strict = strict || def.strict
	}
	defer func() {
		if park != "" {
			return // a parked call has no result
		}
		if edited != "" {
			// An edited call's EDITED line is part of the result the
			// model reads, so the cap covers it too (rule 8) — never
			// cutting into the pinned line itself.
			res.Content = edited + res.Content
			if resultCap > 0 && resultCap < len(editedPrefix) {
				resultCap = len(editedPrefix)
			}
		}
		res.Content, cut = capResultN(res.Content, resultCap)
	}()

	// mark belongs to this call's chain alone, but not to one goroutine:
	// a middleware may call next more than once at a time (hedging) or
	// abandon it on a deadline of its own while the handler returns
	// late, so every field is read and written under its lock.
	mark := new(handlerMark)
	mark.wantStack = func() bool { return a.panicStackWanted(ctx, rec) }
	// Under the development detail level (otel.DevDetail) each layer of
	// the chain gets its own span; otherwise the chain is the plain one.
	var layers *toolLayerSpans
	if (len(a.toolMW) > 0 || (def != nil && len(def.mw) > 0)) && a.obs.devDetailMarked() {
		layers = &toolLayerSpans{tracer: a.obs.tracer, rec: rec}
	}
	fn := a.chain(def, strict, parkSet, mark, layers)
	var out string
	if timeout <= 0 {
		out, fail = invokeContained(ctx, fn, call, mark, nil)
	} else {
		out, fail = invokeWithTimeout(ctx, fn, call, timeout, mark)
	}
	// The chain has returned: the loop settles the call now. A handler
	// a layer let go of before it started (a middleware that gave up on
	// next, a goroutine it left behind) never starts afterwards — the
	// answer read below stays true (enter refuses, as for a Timeout).
	mark.abandon()
	if fail.err != nil {
		if fail.park && !ranPastHandler(def, mark) {
			park = mark.parked()
			if park == "" {
				park = parkByMiddleware
			}
			return res, park, false, 0, toolFailure{}
		}
		if text, unknown := outcomeUnknown(ctx, call, def, fail, mark, timeout); unknown {
			// A call with an effect whose handler started and gave no
			// answer of its own: its outcome is unknown, and the result
			// says exactly that (ADR 0032, WR-6) — or, under
			// ParkUnknown, the call parks for its caller's ladder.
			if parkUnknownFrom(ctx) {
				return res, parkByUnknown, false, 0, toolFailure{}
			}
			fail.text = text
			res.IsError = true
			res.Content = text
			return res, "", false, 0, fail
		}
		res.IsError = true
		res.Content = fail.text
		return res, "", fail.retry, 0, fail
	}
	res.Content = out
	return res, "", false, 0, toolFailure{}
}

// toolFailure is a failed call's error with every fact the loop and the
// observer read of it: read once, by settleToolError, so a panic in the
// error's own methods (Error, Is, As, ErrorType — user code) is
// contained with the call's other failures instead of crashing the
// process from the tool goroutine (AGENTS rule 1, P9-errortype). The
// zero value is a call that did not fail.
type toolFailure struct {
	err error
	// park: the error wraps ErrApprovalRequired — the call is pending,
	// not failed. deadline: it matches context.DeadlineExceeded (the
	// Timeout path names it). retry: a RETRY ToolError, counted through
	// errors.As so a middleware's counts like a handler's (the chain's
	// outcome is what the loop sees).
	park, deadline, retry bool
	// expired: the error matched context.DeadlineExceeded after the
	// call's own Timeout had passed (settleToolError's expired). Nothing
	// else was read — the Timeout path reports the timeout instead, as
	// it did before any other fact was read (the deadline match comes
	// first, ahead of the park check).
	expired bool
	// text is the result the model sees (Error()).
	text string
	// errType, panicLayer and inputKind are the execute_tool span's
	// error.type, weft.panic.layer and weft.tool.input_error.
	errType, panicLayer, inputKind string
	// panicStack is a contained panic's stack when the development
	// detail level kept it (the weft.panic event), nil otherwise.
	panicStack []byte
}

// settleToolError reads err's facts. A panic while reading them is the
// tool's failure: the facts become those of the contained-panic error
// (the same "tool X panicked: v" result a handler's panic gets),
// attributed to the handler when err is the error the handler returned
// and to the middleware otherwise. Read whether or not a span records
// the call, so the model sees the same result with or without a tracer
// (observability never changes behaviour, ADR 0016).
//
// The attribution has limits, and they are the span's alone (the
// model-visible result is the same "panicked" text whichever layer it
// names): the error compared is the last one the handler returned — a
// middleware that called next more than once is judged against the
// last call to return — and the comparison is samePanicValue's, so a
// middleware that builds a new error of the handler's own uncomparable
// type (a struct holding a slice, say) reads as handler.
//
// expired, when not nil, is the Timeout path's question "has the
// call's own deadline passed while the run's has not?". It is asked
// first, before the park check and before Error, for an error matching
// context.DeadlineExceeded: when it answers yes the call timed out, the
// failure is marked expired and nothing else of the error is read —
// the order the Timeout path always had (invokeWithTimeout).
func settleToolError(name string, err error, mark *handlerMark, expired func() bool) (f toolFailure) {
	defer func() {
		if p := recover(); p != nil {
			layer := panicLayerMiddleware
			if mark.returned(err) {
				layer = panicLayerHandler
			}
			f = readToolError(&toolPanicError{name: name, value: p, layer: layer})
		}
	}()
	if expired != nil && errors.Is(err, context.DeadlineExceeded) && expired() {
		return toolFailure{err: err, deadline: true, expired: true}
	}
	return readToolError(err)
}

// readToolError is settleToolError's reading, uncontained: on core's
// own errors (a contained panic, a timeout, the run's cancellation) it
// cannot panic — fmt contains a panic in a panic value's String or
// Error. Its order is the loop's: the park check, the text, the RETRY
// code, then the span's facts. Its errors.AsType targets live on the
// stack (errors.As would box one per target on the heap: an untraced
// failed call reads every fact too).
func readToolError(err error) toolFailure {
	_, dispatched := errors.AsType[*dispatchedParkError](err)
	f := toolFailure{err: err, park: errors.Is(err, ErrApprovalRequired) && !dispatched}
	if f.park {
		return f
	}
	f.text = err.Error()
	te, isTE := errors.AsType[*ToolError](err)
	f.retry = isTE && te.Code == CodeRetry
	f.deadline = errors.Is(err, context.DeadlineExceeded)
	pe, isPE := errors.AsType[*toolPanicError](err)
	if isPE {
		f.panicLayer, f.panicStack = pe.layer, pe.stack
	}
	f.errType = classifyToolError(err, te, isTE, isPE)
	f.inputKind = inputErrorKind(err)
	return f
}

// callTimeout is the deadline a call to def runs under: its own
// Timeout, else the agent's (0: none).
func (a *Agent) callTimeout(def *ToolDef) time.Duration {
	if def != nil && def.timeoutSet {
		return def.timeout
	}
	return a.toolTimeout
}

// callCap is the result cap a call to def runs under: its own
// MaxResultBytes, else the agent's (0: none).
func (a *Agent) callCap(def *ToolDef) int {
	if def != nil && def.capSet {
		return def.resultCap
	}
	return a.resultCap
}

// safeKey asks the run's IdempotencyKeys function for c's key, contained:
// a panic names no key, and the call fails closed — an error wrapping
// ErrNotDispatched, its handler never started (rule 1: the run goes on)
// — reported as one Warn line.
func (a *Agent) safeKey(ctx context.Context, keys func(Call) string, c Call) (key string, err error) {
	defer func() {
		if p := recover(); p != nil {
			key = ""
			err = fmt.Errorf("%w: the run's IdempotencyKeys function panicked naming the call's key: %v", ErrNotDispatched, p)
			if l := a.obs.logger(); l.Enabled(ctx, slog.LevelWarn) {
				l.LogAttrs(ctx, slog.LevelWarn, "idempotency key function panicked: the call is not dispatched",
					slog.String(logRun, c.RunID), slog.String(logCall, c.CallID), slog.String(logTool, c.Name), slog.String(logErr, fmt.Sprint(p)))
			}
		}
	}()
	return keys(c), nil
}

// toolTimeoutError is a per-call timeout, typed so the observer can
// classify it as error.type "timeout" without matching text; Error
// preserves the pinned model-visible string (ADR 0002).
type toolTimeoutError struct {
	name string
	d    time.Duration
}

func (e *toolTimeoutError) Error() string {
	return fmt.Sprintf("tool %q timed out after %s", e.name, e.d)
}

// toolPanicError is a contained tool panic, typed so the observer
// classifies it as error.type "panic" with the layer it came from
// (weft.panic.layer: "handler" or "middleware") without matching text;
// Error preserves the pinned model-visible string (ADR 0002). It wraps
// nothing: the panic value is reported, not unwrapped, exactly as the
// untyped error before it.
type toolPanicError struct {
	name  string
	value any
	layer string
	// stack is the panicking goroutine's stack, kept only under the
	// development detail level with content on (handlerMark.wantStack);
	// never in Error.
	stack []byte
}

func (e *toolPanicError) Error() string {
	return fmt.Sprintf("tool %q panicked: %v", e.name, e.value)
}

// The weft.panic.layer values.
const (
	panicLayerHandler    = "handler"
	panicLayerMiddleware = "middleware"
)

// invokeContained runs the chain, turning a panic — in the handler or
// in middleware — into an error, and settles the chain's error
// (settleToolError: a panic in its methods is contained too).
// Containment sits outside the chain by design: no middleware can turn
// a panic into a run failure. mark is the chain's handler mark (chain):
// set when the panic unwound through the handler; nil reads as
// middleware. expired is settleToolError's (nil off the Timeout path).
func invokeContained(ctx context.Context, fn ToolCaller, call ToolCallPart, mark *handlerMark, expired func() bool) (string, toolFailure) {
	out, err := invokeChain(ctx, fn, call, mark)
	if err != nil {
		return out, settleToolError(call.Name, err, mark, expired)
	}
	return out, toolFailure{}
}

// invokeChain is invokeContained's containment of the chain itself.
func invokeChain(ctx context.Context, fn ToolCaller, call ToolCallPart, mark *handlerMark) (out string, err error) {
	defer func() {
		if p := recover(); p != nil {
			layer := panicLayerMiddleware
			if mark.from(p) {
				layer = panicLayerHandler
			}
			pe := &toolPanicError{name: call.Name, value: p, layer: layer}
			if mark != nil && mark.wantStack != nil && mark.wantStack() {
				// Still on the panicking goroutine's stack: the frames
				// that panicked are below this deferred call.
				pe.stack = debug.Stack()
			}
			err = pe
		}
	}()
	return fn(ctx, call)
}

// handlerMark is one call's record of a panic out of its handler
// (chain's base notes it while unwinding). invokeContained reads the
// layer from it: the panic it recovered is the handler's when the
// handler panicked with that same value — still unwinding, or
// recovered and re-raised by a middleware — and the middleware's
// otherwise, including a middleware that recovered the handler's panic
// and later panicked with a value of its own.
//
// The base may run on several goroutines at once — a middleware that
// calls next twice concurrently (hedging), or one that abandons next on
// a deadline of its own while the handler returns late — so every field
// is written and read under mu. The panic value and the error are the
// last written; whether some entry is still running (inFlight) and
// whether some entry answered with no error (answeredNil) are kept
// across every entry, so a middleware that retries next reads the
// whole of what its attempts did, not the last attempt alone.
type handlerMark struct {
	mu       sync.Mutex
	panicked bool
	value    any
	// parkedBy is the approval boundary's reason when the chain's base
	// parked the call (parkByApproval, or the park rule's); a parked
	// call whose base never ran was parked by a middleware.
	parkedBy string
	// err is the error the handler last returned (nil: none, or it
	// never returned).
	err error
	// answered: some entry of the handler returned (with err) — it did
	// not panic. answeredNil: some entry returned no error — that entry
	// took effect, whatever a layer around it, or a later entry, did
	// after (outcomeUnknown). lostAnswer: some entry returned an error
	// matching ErrOutcomeUnknown.
	answered, answeredNil, lostAnswer bool
	// inFlight counts the entries of the handler that have started and
	// neither returned nor panicked yet: a layer that fails while one
	// still runs leaves an effect that may yet happen.
	inFlight int
	// entered: the call passed every middleware, the approval check
	// and the dispatch gate, and its handler was called — it may have
	// taken effect.
	entered bool
	// abandoned: the loop gave up on the call (its Timeout, or the
	// run's cancellation under it) before the handler was entered; it
	// never is (enter refuses).
	abandoned bool
	// settled: the loop has given the call its answer (abandon was
	// called, whether or not the handler had been entered). A CallTool
	// from the handler — still running past its Timeout, or a goroutine
	// it left behind — is refused from then on (innerDispatch).
	settled bool
	// wantStack, when set, is asked once a panic is recovered: true
	// under the development detail level with content on, and the
	// containment keeps the panicking goroutine's stack (the
	// weft.panic event, layers.go). Never asked on a call that does
	// not panic, so the default path pays nothing.
	wantStack func() bool
}

func (m *handlerMark) note(p any) {
	m.mu.Lock()
	m.panicked, m.value = true, p
	if m.inFlight > 0 {
		m.inFlight--
	}
	m.mu.Unlock()
}

// handlerPanicked reports whether the handler itself panicked (note),
// not a method of the error it returned.
func (m *handlerMark) handlerPanicked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.panicked
}

// enter marks the handler entered, unless the loop has abandoned the
// call first (abandon): false then, and the handler must not run.
func (m *handlerMark) enter() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.abandoned {
		return false
	}
	m.entered = true
	m.inFlight++
	return true
}

// abandon is the loop giving up on the call: its Timeout passed, or
// the run was canceled under it. Decided
// under the same lock as enter, so the two never disagree: a handler
// entered by now may have taken effect (outcome unknown), and one not
// entered by now never is — the timeout the loop answers stays true.
func (m *handlerMark) abandon() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settled = true
	if !m.entered {
		m.abandoned = true
	}
}

// wasSettled reports whether the loop has settled the call (abandon).
func (m *handlerMark) wasSettled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settled
}

func (m *handlerMark) wasEntered() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.entered
}

func (m *handlerMark) park(by string) {
	m.mu.Lock()
	m.parkedBy = by
	m.mu.Unlock()
}

func (m *handlerMark) returning(err error) {
	// Read outside the lock: Is is the error's own code, and a panic in
	// it reads as no match (errIsContained).
	lost := err != nil && errIsContained(err, ErrOutcomeUnknown)
	m.mu.Lock()
	m.err, m.answered = err, true
	if err == nil {
		m.answeredNil = true
	}
	m.lostAnswer = m.lostAnswer || lost
	if m.inFlight > 0 {
		m.inFlight--
	}
	m.mu.Unlock()
}

// effect is what the handler's entries leave behind, across all of
// them: running, some entry is still running (it may yet take effect);
// tookEffect, some entry answered with no error; lost, some entry said
// it lost its answer (ErrOutcomeUnknown); answered, some entry returned
// at all.
func (m *handlerMark) effect() (running, tookEffect, lost, answered bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inFlight > 0, m.answeredNil, m.lostAnswer, m.answered
}

// panicValue is the value the handler last panicked with.
func (m *handlerMark) panicValue() any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.value
}

// parked is parkedBy, read under the lock.
func (m *handlerMark) parked() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.parkedBy
}

// returned reports whether err is the error the handler last returned —
// the layer a panic in its methods belongs to. An uncomparable error
// value of the handler's type reads as the handler's (samePanicValue's
// rule).
func (m *handlerMark) returned(err error) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	last := m.err
	m.mu.Unlock()
	return last != nil && samePanicValue(err, last)
}

// from reports whether the recovered value p is the handler's panic.
// A value of an uncomparable type (a slice, a map) cannot be compared:
// it reads as the handler's when the handler panicked with one of the
// same type.
func (m *handlerMark) from(p any) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	panicked, value := m.panicked, m.value
	m.mu.Unlock()
	if !panicked {
		return false
	}
	tp, tv := reflect.TypeOf(p), reflect.TypeOf(value)
	if tp != tv {
		return false
	}
	if tp != nil && !tp.Comparable() {
		return true
	}
	return samePanicValue(p, value)
}

// samePanicValue is p == q, true when the comparison itself panics (a
// comparable struct holding an uncomparable value): same type, so the
// handler's by the rule above.
func samePanicValue(p, q any) (same bool) {
	defer func() {
		if recover() != nil {
			same = true
		}
	}()
	return p == q
}

// invokeWithTimeout runs the tool under a per-call deadline. The handler
// sees the deadline on its ctx; if it has not returned when the deadline
// passes, the call is recorded as timed out and the handler's goroutine
// is abandoned — a hung tool must not hang the run. A cancellation of
// the run's own ctx is reported as that cancellation, not as a timeout.
func invokeWithTimeout(ctx context.Context, fn ToolCaller, call ToolCallPart, timeout time.Duration, mark *handlerMark) (string, toolFailure) {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	done := make(chan timedOutcome, 1)
	// expired is the deadline fact, asked first of an error matching
	// DeadlineExceeded (settleToolError): a call whose own deadline has
	// passed timed out, whatever else its error says — parked or not,
	// and without its Error being read.
	expired := func() bool { return ctx.Err() == nil && tctx.Err() != nil }
	dl, _ := tctx.Deadline()
	go func() {
		out, fail := invokeContained(tctx, fn, call, mark, expired)
		done <- timedOutcome{out, fail, time.Now()}
	}()
	// The timeout and the run's cancellation are core's own errors:
	// read uncontained, they cannot panic.
	timedOut := func() toolFailure {
		return readToolError(&toolTimeoutError{name: call.Name, d: timeout})
	}
	if o, answered := awaitAnswer(done, tctx.Done(), dl); answered {
		// A handler that honoured the deadline returns
		// DeadlineExceeded itself; name the timeout rather than
		// echoing the context error. The match was read in the
		// handler's goroutine, contained (settleToolError), before
		// anything else of the error.
		if o.fail.deadline && expired() {
			return "", timedOut()
		}
		if o.fail.expired {
			// The run's own ctx ended between the goroutine's answer
			// and this one: the error is the call's, read in full now
			// (contained, as on the goroutine).
			return o.out, settleToolError(call.Name, o.fail.err, mark, nil)
		}
		return o.out, o.fail
	}
	// Whether the handler started is settled now, for good: one still
	// in the chain — a slow middleware, a slow gate — never starts
	// after this answer.
	if mark != nil {
		mark.abandon()
	}
	if err := ctx.Err(); err != nil {
		return "", readToolError(err)
	}
	return "", timedOut()
}

// timedOutcome is a call's answer on invokeWithTimeout's channel.
type timedOutcome struct {
	out  string
	fail toolFailure
	// at is when the handler's chain returned: an answer counts only
	// when it came by the deadline (awaitAnswer).
	at time.Time
}

// awaitAnswer waits for the call's answer or its deadline, whichever
// comes first. An answer the handler gave by the deadline (dl) is kept
// even when both are ready at once — a select alone picks between ready
// cases at random, and would throw away the real result of a handler
// that answered right at its Timeout (C9). An answer given after the
// deadline is not an answer within the Timeout: most often it is the
// handler reacting to the deadline's own cancellation, and the call
// timed out (answered false), whichever case the select saw first.
func awaitAnswer(done <-chan timedOutcome, deadline <-chan struct{}, dl time.Time) (timedOutcome, bool) {
	inTime := func(o timedOutcome) (timedOutcome, bool) {
		if !dl.IsZero() && o.at.After(dl) {
			return timedOutcome{}, false
		}
		return o, true
	}
	select {
	case o := <-done:
		return inTime(o)
	case <-deadline:
		select {
		case o := <-done:
			return inTime(o)
		default:
			return timedOutcome{}, false
		}
	}
}

// capResult enforces a tool-result cap. The cut lands on a rune
// boundary and always ends with a marker naming the bytes the model did
// not receive, so the model knows the output is partial and how much of
// it is missing rather than silently receiving a prefix.
func capResult(s string, resultCap int) string {
	out, _ := capResultN(s, resultCap)
	return out
}

// capResultN is capResult reporting the bytes it removed (0 when it cut
// nothing) — the number its marker names, weft.tool.truncated_bytes.
func capResultN(s string, resultCap int) (string, int) {
	if resultCap <= 0 || len(s) <= resultCap {
		return s, 0
	}
	cut := resultCap
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + fmt.Sprintf("\n…[truncated %d bytes]", len(s)-cut), len(s) - cut
}

// outcomeUnknown reports whether a failed call's outcome is unknown,
// and the pinned text the model reads for it (ADR 0032, WR-6, and its
// 2026-11-05 amendment): a tool that is not ReplaySafe whose handler
// started and gave no answer of its own — its Timeout passed (the
// handler is abandoned and may still complete), the run was canceled
// under it (a cancel, an interrupt, a close), it panicked (whatever it
// did first, it did), it said so (ErrOutcomeUnknown), or a layer around
// it gave up on it while it still ran — and one whose handler answered
// with no error while a layer around it (a WrapTools middleware, the
// AfterDispatch hook) then failed: the effect happened, the answer never
// reached the model. A failure the handler returned while the run was
// live is its own answer and keeps its words, whatever a middleware
// made of it after. A layer that entered the handler more than once (a
// middleware retrying next) is judged on every entry: unknown while
// any entry still runs, or when any entry answered with no error or
// lost its answer — a plain failure only when every entry returned an
// error of its own.
func outcomeUnknown(ctx context.Context, call ToolCallPart, def *ToolDef, fail toolFailure, mark *handlerMark, timeout time.Duration) (string, bool) {
	if def == nil || def.ReplayPolicy() == ReplaySafe || mark == nil || !mark.wasEntered() {
		return "", false
	}
	if _, ok := errAsContained[*toolTimeoutError](fail.err); ok {
		return fmt.Sprintf("%s: tool %q did not answer within %s; it may still complete; outcome unknown", CodeOutcomeUnknown, call.Name, timeout), true
	}
	if ctx.Err() != nil {
		return fmt.Sprintf("%s: tool %q was cut off before it answered; it may have run; outcome unknown", CodeOutcomeUnknown, call.Name), true
	}
	if mark.handlerPanicked() {
		// The handler was entered and panicked: whatever it did before
		// the panic, it did — the effect is unknown, not failed. The
		// value named is the one the containment caught, else (a
		// middleware recovered it and failed otherwise) the handler's.
		v := mark.panicValue()
		if pe, ok := errAsContained[*toolPanicError](fail.err); ok {
			v = pe.value
		}
		return fmt.Sprintf("%s: tool %q panicked before it answered (%v); it may have run; outcome unknown", CodeOutcomeUnknown, call.Name, v), true
	}
	if errIsContained(fail.err, ErrOutcomeUnknown) {
		return fmt.Sprintf("%s: tool %q lost its answer (%s); it may have run; outcome unknown", CodeOutcomeUnknown, call.Name, fail.text), true
	}
	// Read across every entry of the handler, not the last one: a
	// middleware that retries next may give up on a second attempt still
	// running after a first that failed, or fail after a first attempt
	// that answered.
	running, tookEffect, lost, answered := mark.effect()
	switch {
	case running || !answered:
		// A layer around the handler (a middleware's own deadline, say)
		// gave up on it while it still ran.
		return fmt.Sprintf("%s: tool %q had not answered when a layer around it failed (%s); it may still complete; outcome unknown", CodeOutcomeUnknown, call.Name, failText(fail)), true
	case tookEffect:
		return fmt.Sprintf("%s: tool %q answered, but a layer around it failed after (%s); it may have taken effect; outcome unknown", CodeOutcomeUnknown, call.Name, failText(fail)), true
	case lost:
		// The handler said it lost its answer; a middleware masked that.
		return fmt.Sprintf("%s: tool %q lost its answer (%s); it may have run; outcome unknown", CodeOutcomeUnknown, call.Name, failText(fail)), true
	}
	return "", false
}

// ranPastHandler reports whether a call parked by a layer around its
// handler (ErrApprovalRequired from a middleware) had already run it: a
// tool that is not ReplaySafe whose handler started and did not answer
// with an error of its own (any entry of it still running, or any
// answering with no error — handlerMark.effect). Such a call is not parked — approving it
// would run the effect again — but read as of unknown outcome. A
// handler's own ErrApprovalRequired still parks its call.
func ranPastHandler(def *ToolDef, mark *handlerMark) bool {
	if def == nil || def.ReplayPolicy() == ReplaySafe || mark == nil || !mark.wasEntered() {
		return false
	}
	running, tookEffect, _, answered := mark.effect()
	return running || tookEffect || !answered
}

// failText is the failure's text, read contained when the settled
// failure carries none (a park's error).
func failText(f toolFailure) (text string) {
	if f.text != "" || f.err == nil {
		return f.text
	}
	defer func() {
		if p := recover(); p != nil {
			text = fmt.Sprintf("an error whose Error method panicked: %v", p)
		}
	}()
	return f.err.Error()
}

// errIsContained is errors.Is(err, target), contained: a panic in the
// error's own Is reads as no match — the error's failure is the
// contained panic's, the plain "panicked" result rule 1 pins.
func errIsContained(err, target error) (is bool) {
	defer func() {
		if recover() != nil {
			is = false
		}
	}()
	return errors.Is(err, target)
}

// errAsContained is errors.AsType with a panic in the error's own As
// or Unwrap contained (rule 1): it reads as no match.
func errAsContained[T error](err error) (v T, ok bool) {
	defer func() {
		if recover() != nil {
			var zero T
			v, ok = zero, false
		}
	}()
	return errors.AsType[T](err)
}

type parkUnknownKey struct{}

func withParkUnknown(ctx context.Context) context.Context {
	return context.WithValue(ctx, parkUnknownKey{}, true)
}

func parkUnknownFrom(ctx context.Context) bool {
	on, _ := ctx.Value(parkUnknownKey{}).(bool)
	return on
}
