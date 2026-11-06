package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
)

// RunOption configures a single run.
type RunOption interface {
	applyRun(*runConfig)
}

type runConfig struct {
	id            string
	messages      []Message
	thinking      ThinkingConfig
	thinkingSet   bool // a run-level Thinking option was applied
	toolChoice    ToolChoiceConfig
	toolChoiceSet bool // a run-level ToolChoice option was applied
	params        RequestParams
	paramsSet     bool // a run-level Params option was applied
	// decisions resolves calls left pending by an earlier run: call id →
	// approve, deny with a reason, or resolve with content
	// (Approve, Deny, Resolve, ResolveError).
	decisions map[string]decision
	// resumedFrom is ResumeOf's run id: the parked run this one
	// resumes, stamped as weft.resumed_from on the invoke_agent span
	// and the run_start record. "" when the caller named none.
	resumedFrom string
	// steer is the run's steering source, installed by Steering; nil on
	// an ordinary run, and never inherited by a Subagent's child runs.
	steer SteerFunc
	// onMessages holds the run's transcript observers, installed by
	// OnMessages; the loop calls them as messages join the transcript.
	// Empty on an ordinary run, and never inherited by a child.
	onMessages []func(context.Context, int, []Message)
	// keys names each executed call's IdempotencyKey (IdempotencyKeys);
	// nil means the run-scoped key (defaultIdempotencyKey). Never
	// inherited by a child run.
	keys func(Call) string
	// dispatch is BeforeDispatch's gate; nil on an ordinary run, never
	// inherited by a child run.
	dispatch DispatchFunc
	// afterDispatch is AfterDispatch's hook; nil on an ordinary run,
	// never inherited by a child run.
	afterDispatch AfterDispatchFunc
	// parkUnknown parks a call whose outcome is unknown instead of
	// answering it (ParkUnknown); never inherited by a child run.
	parkUnknown bool
	// metadata is the run's own caller pairs, merged across its Metadata
	// options (later key winning); execute overlays it on the context's
	// inherited metadata before any span starts (metadata.go).
	metadata map[string]string
	// The dual options' run-side values (Instructions, MaxSteps,
	// Parallelism): a *Set flag per knob, the Thinking shape. execute
	// validates the lower-only rules (a raise is ErrInvalidRunOption,
	// before any model call) and resolves the effective values.
	system      string
	systemSet   bool
	maxSteps    int
	maxStepsSet bool
	// keptSteps is KeptSteps: how many of the turn's steps the input
	// already ends with (a replay's kept prefix) — counted toward the
	// turn's guards, never re-run.
	keptSteps      int
	parallelism    int
	parallelismSet bool
	// onlyTools narrows the run to the named tools among the agent's
	// registered ones (narrowing only; an unknown name fails the run
	// before any model call). nil keeps the full set.
	onlyTools []string
	// model replaces the agent's model for this run; the WrapModel chain
	// is rebuilt over it. nil keeps the agent's.
	model Model
	// parkOn parks calls to the named tools at the approval boundary,
	// exactly as RequireApproval would (ADR 0007 applied per run).
	parkOn []string
	// parkAll is ParkAllExcept: every call parks unless its tool is in
	// parkExcept — the names every ParkAllExcept option of the run lists
	// (non-nil once set, possibly empty).
	parkAll    bool
	parkExcept map[string]bool
	// parkSourced is what the agent's ParkSources answered for this run
	// (ADR 0030), read once in execute: parked like parkOn, recorded
	// apart from it (weft.tool.pending.source park_source) and never an
	// override.
	parkSourced []string
	// parkSourceFailed: a ParkSource panicked for this run — its set is
	// unknown, so every call parks (fail safe, WR-14), reported as
	// park_source.
	parkSourceFailed bool
}

type decision struct {
	approved bool
	// by and grant are an approval's provenance (ApproveBy): reporting
	// only, stamped on the approved call's execute_tool span.
	by, grant string
	reason    string
	// A resolved call never runs its handler: content becomes the
	// ToolResultPart verbatim (isError marks ResolveError). A decision
	// is exactly one of approve / deny / resolve (ADR 0007's
	// 2026-09-22 amendment).
	content  string
	resolved bool
	isError  bool
	// args, on an approval, replaces the call's arguments for its
	// execution (ApproveEdited): the decider edited them.
	args json.RawMessage
}

func (c *runConfig) decide(id string, d decision) {
	if c.decisions == nil {
		c.decisions = map[string]decision{}
	}
	c.decisions[id] = d
}

type approveOption struct{ id, by, grant string }

func (o approveOption) applyRun(c *runConfig) {
	c.decide(o.id, decision{approved: true, by: o.by, grant: o.grant})
}

// Approve resumes a call left on RunResult.Pending by an earlier run:
// pass the earlier transcript with Messages and the decision, and the
// loop executes the call — through the ordinary tool chain, with
// Call.Approved set — before its next model call. Ids that are not
// pending are ignored.
func Approve(callID string) RunOption { return approveOption{id: callID} }

// ApproveBy is Approve naming who or what approved the call: by is the
// decider's class — weft/thread's words are user, grant, approver,
// quorum and policy — and grantID the standing permission that decided,
// when one did. Reporting only: the call resumes exactly as Approve
// resumes it, and its execute_tool span carries weft.tool.approved_by
// (by) and weft.grant.id (grantID), each absent when empty — ids and
// enums, never a person's name, so put an opaque class in by. The core
// cannot know them (a transcript names no decider); weft/thread passes
// them on every resume it runs. The last decision for an id wins, as
// with Approve.
func ApproveBy(callID, by, grantID string) RunOption {
	return approveOption{id: callID, by: by, grant: grantID}
}

type approveEditedOption struct {
	id, by string
	args   json.RawMessage
}

func (o approveEditedOption) applyRun(c *runConfig) {
	c.decide(o.id, decision{approved: true, by: o.by, args: append(json.RawMessage(nil), o.args...)})
}

// ApproveEdited approves a pending call with the arguments its decider
// edited: the call executes like an Approve — through the tool chain,
// Call.Approved set — but on args instead of the arguments the model
// wrote, and its result tells the model so: it starts with the pinned
// line "EDITED: the call ran with the arguments <args>" (ADR 0007's
// 2026-11-04 amendment; the model's own call stays as it wrote it in
// the transcript). by names the decider's class, as ApproveBy's. The
// arguments are validated as any call's are (decode, StrictInput): a
// call whose edited arguments do not fit its tool fails as a call with
// bad arguments would. The EDITED line is part of the result, under
// the result cap (MaxResultBytes). An edit covers one resume: the
// transcript keeps the model's call, so a call that parks again in the
// resume (ParkUnknown, a middleware's park) is reported on
// RunResult.Pending with the model's own arguments — what a plain
// Approve of it would run — and runs the edit again only when decided
// again with ApproveEdited (weft/thread does, from its record).
func ApproveEdited(callID string, args json.RawMessage, by string) RunOption {
	return approveEditedOption{id: callID, by: by, args: args}
}

// editedPrefix is the line an edited call's result starts with
// (ApproveEdited), before the edited arguments and a newline.
const editedPrefix = "EDITED: the call ran with the arguments "

type denyOption struct{ id, reason, by, grant string }

func (o denyOption) applyRun(c *runConfig) {
	c.decide(o.id, decision{reason: o.reason, by: o.by, grant: o.grant})
}

// Deny resolves a pending call without running it: the model sees an
// error result reading "DENIED: <reason>" and the loop continues.
// Pending calls given neither Approve nor Deny are denied with the
// reason "no decision" ("DENIED: no decision"). A denied call never
// runs and emits no event; its execute_tool span is zero-length,
// marked weft.tool.denied with weft.tool.denied_kind denied or
// no_decision (never the reason, which is the transcript's).
func Deny(callID, reason string) RunOption { return denyOption{id: callID, reason: reason} }

// DenyBy is Deny naming what refused the call, as ApproveBy names what
// approved one: by "grant" with grantID the standing refusal that
// decided (weft/thread passes it when a deny grant matched). Reporting
// only: the call is denied exactly as Deny denies it, and its
// zero-length execute_tool span carries weft.tool.denied_by "grant" and
// weft.grant.id; any other by — or none, a plain Deny — reads
// weft.tool.denied_by "approval", the approval boundary's refusal. Ids
// and enums, never the reason.
func DenyBy(callID, reason, by, grantID string) RunOption {
	return denyOption{id: callID, reason: reason, by: by, grant: grantID}
}

type resolveOption struct {
	id, content string
	isError     bool
}

func (o resolveOption) applyRun(c *runConfig) {
	c.decide(o.id, decision{content: o.content, resolved: true, isError: o.isError})
}

// Resolve resumes a pending call with a result computed outside the
// process — the human-as-tool-executor shape: run the query in prod,
// paste what happened, and the next model call sees it. The content
// becomes the call's ToolResultPart verbatim; the handler never runs,
// no execute_tool span or ToolStart/ToolFinish is emitted (nothing
// executed), and Call.Approved is never set. MaxResultBytes applies
// as to any result. Composes with Approve and Deny in one resuming
// call; the last option for an id wins.
//
// Resolve on a call that is not pending in the resumed transcript is a
// loud run error at step 0 — a deliberate asymmetry with Approve and
// Deny, which ignore unknown ids: those are yes/no marks over an id
// set, while Resolve carries a payload the caller expects the model to
// see, and dropping it silently is the one thing the error model
// forbids (ADR 0007's 2026-09-22 amendment).
func Resolve(callID, content string) RunOption {
	return resolveOption{id: callID, content: content}
}

// ResolveError is Resolve with the result marked as an error: the
// model sees the content on an error result, the shape a failed
// execution would have produced.
func ResolveError(callID, content string) RunOption {
	return resolveOption{id: callID, content: content, isError: true}
}

type resumeOfOption string

func (o resumeOfOption) applyRun(c *runConfig) {
	if o != "" {
		c.resumedFrom = string(o)
	}
}

// ResumeOf names the run this run resumes: the parked run whose
// RunResult.ID the caller holds when it passes that run's transcript
// back with Approve, Deny or Resolve. The core cannot infer it — a
// transcript carries no run id — so the caller states it, and the
// run's invoke_agent span and run_start record carry it as
// weft.resumed_from, the link a reader follows from a decided park to
// the run that resumed it (record contract §3; a reader's run row reads
// resumed_from, and resumed_by on the parked run). Reporting only: the
// option changes nothing the model sees and nothing the loop does —
// the decisions resume the calls, with or without it. weft/thread
// passes it on every resume turn, weftbase/runtime on every resume it
// runs; an app resuming a plain Generate passes res.ID. Empty values
// are ignored; the last ResumeOf wins. A Subagent's child run does not
// inherit it.
func ResumeOf(runID string) RunOption { return resumeOfOption(runID) }

type runIDOption string

func (o runIDOption) applyRun(c *runConfig) { c.id = string(o) }

// RunID sets the run's identifier instead of generating one — for
// replays, idempotent retries, and correlating with an outer system's own
// ids. Empty values are ignored.
func RunID(id string) RunOption { return runIDOption(id) }

type keptStepsOption int

func (o keptStepsOption) applyRun(c *runConfig) {
	if o > 0 {
		c.keptSteps = int(o)
	}
}

// KeptSteps says the run continues a turn whose first n steps its
// input already holds: the run's Messages end with those steps (each
// one assistant message and its tool results), kept from an earlier
// run of the same turn — a replay from step n (weftbase/runtime's
// from_step) is fed the source's steps 0..n-1 that way. The kept steps
// count toward the turn's guards as if this run had made them:
// MaxSteps budgets the whole turn (this run makes at most MaxSteps − n
// model calls, none when n reaches it), and DetectLoops' streak starts
// from the kept steps' tool calls, so an unchanged replay stops where
// the turn it continues stopped. The usage limit and MaxModelRetries
// count this run's own spend and retries only (the kept steps' usage is
// not in the input), and StopWhen conditions see this run's own
// StepRecords. The run's own steps are still indexed from 0 —
// StepStart, StepRecord and RunError name them, not the turn's
// ordinals. n ≤ 0 is ignored.
func KeptSteps(n int) RunOption { return keptStepsOption(n) }

type onlyToolsOption struct{ names []string }

func (o onlyToolsOption) applyRun(c *runConfig) {
	if len(o.names) > 0 {
		c.onlyTools = append(c.onlyTools, o.names...)
	}
}

// OnlyTools narrows this run to the named tools among the agent's
// registered ones — the ToolSource snapshot when one exists, fetched
// fresh per step as ever. Narrowing only: the playground cannot add a
// tool, because a new tool is code. A name the agent does not have
// fails the run with ErrInvalidRunOption before any model call. The
// step's advertisement and its dispatch resolve against the same
// narrowed snapshot, so what the model was shown is exactly what runs;
// calls to a tool a PrepareStep function dropped fail as unknown, as
// today. Manifest and Agent.Tools keep reporting the static set: they
// describe the code, not one run's experiment (WEFT-PLAYGROUND §10.1
// [D5]). With no names, the run keeps the agent's full set. Several
// OnlyTools options add up: the run keeps every tool any of them names.
// The narrowing holds for an Agent.CallTool from inside the run's tool
// handlers too: a narrowed-away tool is unknown there (ErrNoSuchTool),
// as it is to the loop. A Subagent's child run does not inherit it.
//
// On an agent built with Output[T] the submit_output tool is one of the
// registered tools: OnlyTools narrows it away unless it is named
// (OnlyTools("lookup", "submit_output")), and the run can then not end
// on a valid output. ParkAllExcept, by contrast, never parks it. A call
// to a narrowed-away tool still passes through the agent's WrapTools
// middleware before it is answered as unknown (the chain runs for
// unknown tools, so Allow and Audit see the attempt): an mw.Allow that
// refuses the name answers `DENIED: tool "x" is not allowed` first, and
// the model reads that instead of NO_SUCH_TOOL.
func OnlyTools(names ...string) RunOption { return onlyToolsOption{names} }

type useModelOption struct{ m Model }

func (o useModelOption) applyRun(c *runConfig) {
	if o.m != nil && !isNilModel(o.m) {
		c.model = o.m
	}
}

// UseModel replaces the agent's model for this run, rebuilding the
// WrapModel chain over it (first registered = outermost, the New rule):
// the run's model calls go through the same middleware over m. Pass a
// model the runtime registered as an allowed alternate; a nil model is
// ignored. RunStart.Model and the chat spans report the run's model
// (middleware forwards Info); Agent.Model and the manifest keep naming
// the agent's own (WEFT-PLAYGROUND §10.1 [D5]).
func UseModel(m Model) RunOption { return useModelOption{m} }

type parkOnOption struct{ names []string }

func (o parkOnOption) applyRun(c *runConfig) {
	if len(o.names) > 0 {
		c.parkOn = append(c.parkOn, o.names...)
	}
}

// ParkOn parks a call to any of the named tools at the approval
// boundary (ADR 0007), exactly as if the tool had been built with
// RequireApproval: the call gets its ToolStart and no ToolFinish, the
// run ends successfully with the call on RunResult.Pending, and
// Approve/Deny/Resolve on a resuming run decide it. This is how a
// breakpoint or side-effect parking reaches a runtime-started run
// without touching the agent, which is immutable after New (ADR 0024
// D7, WEFT-PLAYGROUND §10.1). With no names, nothing parks. Names are
// not validated — a name no tool carries parks nothing — and the set
// covers this run only: a Subagent's child run does not inherit it.
// ParkAllExcept is the default-deny form, for when the tools to park
// cannot all be named.
func ParkOn(tools ...string) RunOption { return parkOnOption{tools} }

type parkAllExceptOption struct{ names []string }

func (o parkAllExceptOption) applyRun(c *runConfig) {
	// Every option is its own "park all but these" rule and the rules
	// add up toward parking, so a second option keeps only the names
	// the first one also let through.
	keep := make(map[string]bool, len(o.names))
	for _, name := range o.names {
		if !c.parkAll || c.parkExcept[name] {
			keep[name] = true
		}
	}
	c.parkAll, c.parkExcept = true, keep
}

// ParkAllExcept parks, at the approval boundary (ADR 0007), every tool
// call of the run whose tool is not named — ParkOn turned around: the
// caller lists what may run, and everything else waits for a decision.
// It is the rule for a run that must not fire a side effect nobody
// vouched for (a playground re-run, WEFT-PLAYGROUND §6 rule 3; ADR 0024
// D7), where a list of tools to park cannot be complete:
//
//   - The rule is applied by name to the tool each call resolves to in
//     its step's dispatch snapshot, so a tool only a ToolSource supplies
//     — absent from Agent.Tools and the manifest — parks like any other.
//   - It reaches the runs started inside this run: a Subagent's child
//     run (any run on a tool call's context) applies the same rule to
//     its own tools, where ParkOn stops at the run it was given to. A
//     child that parks ends as a child approval boundary always has —
//     the delegating call's result is SUBAGENT_PENDING (ADR 0014); the
//     parent does not park. Names are matched in parent and child
//     alike, so name a tool only if every tool of that name down the
//     delegation may run. A child run's own ParkAllExcept can narrow
//     the inherited list, never widen it.
//
// A parked call is ParkOn's parked call: its ToolStart and no
// ToolFinish, the run ending successfully with it on RunResult.Pending,
// Approve/Deny/Resolve on the resuming run deciding it — an approved
// call runs once even when the resume carries the rule again, and the
// next call to the tool parks again. The rules only add up toward
// parking: a tool ParkOn names or built with RequireApproval parks
// whether or not it is named here, and several ParkAllExcept options
// let through only the names all of them list. OnlyTools is
// independent — it decides what is offered, this decides what of it
// runs unasked. A name no tool carries is not an error (a ToolSource's
// names are not known up front) and with no names every call parks —
// except an agent's own Output submission (submit_output on an agent
// built with Output, in this run or a child's): it is the run's
// answer, not a side effect, so it never needs naming; ParkOn can
// still park it.
func ParkAllExcept(names ...string) RunOption { return parkAllExceptOption{names} }

type parkSourceOption struct {
	fn func(context.Context) []string
}

func (o parkSourceOption) apply(a *Agent) {
	if o.fn != nil {
		a.parkSources = append(a.parkSources, o.fn)
	}
}

// ParkSource gives the agent a dynamic park set (ADR 0030): fn is read
// once per run, at the run's start — after its run options apply,
// before its first model call — and the names it returns are merged
// into the run's park set exactly as a ParkOn(names...) run option
// would be: a call to a named tool parks at the approval boundary (ADR
// 0007), its ToolStart and no ToolFinish, the run ending successfully
// with the call on RunResult.Pending, Approve/Deny/Resolve on a
// resuming run deciding it. Names match the tool each call resolves to
// in its step's dispatch snapshot, so a tool only a ToolSource supplies
// parks like any other, and a Subagent tool's call parks before its
// child runs. Like ParkOn, the set covers the run it was read for: a
// Subagent's child run reads its own agent's sources, not this one's.
// It is how a platform's debugger breakpoints can reach the runs the
// app starts itself (Weftbase's runtime supplies one).
//
// fn receives the run's context (its metadata placed) and must return
// promptly — it runs on the loop's goroutine before the first model
// call; read a cached set, never the network. A nil or empty answer
// parks nothing. A panic is contained and fails safe: the set it would
// have named is unknown, so the run parks every call (its
// weft.tool.pending.source park_source) but its own Output answer —
// a breakpoint set that cannot be read is not an empty one — and it is
// reported as one Warn line; the next run reads the source again. Several ParkSource options add up; a nil fn is
// ignored. The answer is behaviour, not configuration the run carries:
// it is not an override (no weft.override.* attribute) and a source
// that answers nothing leaves the run byte-identical to one without it.
func ParkSource(fn func(ctx context.Context) []string) Option { return parkSourceOption{fn} }

// ParkSources returns the agent's ParkSource functions in the order
// they were given — a fresh slice, empty when none was set. A satellite
// that installed one reads it back to know the agent carries it (the
// playground runtime reports breakpoints-on-own-runs at registration).
func (a *Agent) ParkSources() []func(ctx context.Context) []string {
	return slices.Clone(a.parkSources)
}

// readParkSources reads the agent's park sources for one run, each
// under its own containment. failed reports a source that panicked: its
// set is unknown, and the run parks every call (WR-14) — a breakpoint
// set that cannot be read is not an empty one.
func (a *Agent) readParkSources(ctx context.Context, runID string) (names []string, failed bool) {
	for _, fn := range a.parkSources {
		got, ok := a.safeParkSource(ctx, runID, fn)
		names = append(names, got...)
		failed = failed || !ok
	}
	return names, failed
}

func (a *Agent) safeParkSource(ctx context.Context, runID string, fn func(context.Context) []string) (names []string, ok bool) {
	defer func() {
		if p := recover(); p != nil {
			names, ok = nil, false
			if l := a.obs.logger(); l.Enabled(ctx, slog.LevelWarn) {
				l.LogAttrs(ctx, slog.LevelWarn, "park source panicked: every call of the run parks",
					slog.String(logRun, runID), slog.String(logErr, fmt.Sprint(p)))
			}
		}
	}()
	return fn(ctx), true
}

// parkRule is a run's park rule, resolved once in execute: the ParkOn
// names, and — when a ParkAllExcept is in force, the run's own or one
// inherited from the run it was started inside — the names that may
// still run. nil parks nothing.
type parkRule struct {
	on     map[string]bool
	all    bool
	except map[string]bool
	// sourced holds the names only a ParkSource put in on (none of the
	// run's own ParkOn options named them): their parks read
	// park_source, not park_on.
	sourced map[string]bool
	// sourceFailed: a ParkSource panicked, so the run parks every call
	// but its own Output answer (park_source).
	sourceFailed bool
}

// The park sources a parked call's execute_tool span names
// (weft.tool.pending.source, UX-H-18), in the order parkedBy decides.
const (
	parkByApproval   = "approval"        // the tool's RequireApproval
	parkByParkOn     = "park_on"         // the run's ParkOn
	parkBySource     = "park_source"     // an agent ParkSource (a breakpoint)
	parkByParkAll    = "park_all_except" // ParkAllExcept, the run's or inherited
	parkByMiddleware = "middleware"      // ErrApprovalRequired from a tool middleware or the handler
	parkByUnknown    = "outcome_unknown" // ParkUnknown: a started call with an effect gave no answer
)

// parkedBy names why the rule parks a call to the named tool, "" when
// it does not: park_on before park_source (a name both list is the
// run's own choice), then the default-deny half.
func (p *parkRule) parkedBy(name string, answer bool) string {
	switch {
	case p == nil:
		return ""
	case p.on[name] && p.sourced[name]:
		return parkBySource
	case p.on[name]:
		return parkByParkOn
	case p.sourceFailed && !answer:
		return parkBySource
	case p.all && !answer && !p.except[name]:
		return parkByParkAll
	}
	return ""
}

// parks reports whether a call to the named tool parks under the rule.
// answer marks the running agent's own Output submission, which the
// default-deny half lets through: it is the run's answer, not a side
// effect. ParkOn naming it still parks it.
func (p *parkRule) parks(name string, answer bool) bool {
	if p == nil {
		return false
	}
	return p.on[name] || (p.sourceFailed && !answer) || (p.all && !answer && !p.except[name])
}

// parkExceptKey is the context key for the ParkAllExcept list in force:
// execute places it so the runs started inside this one inherit it. The
// map is never mutated after it is placed; a present, empty map parks
// everything.
type parkExceptKey struct{}

// resolvePark builds the run's park rule from its own options and the
// except-list inherited on ctx, and returns the context the run's tool
// calls — and so its child runs — carry the list on. A child's own
// list intersects the inherited one: default-deny only tightens on the
// way down.
func (c *runConfig) resolvePark(ctx context.Context) (context.Context, *parkRule) {
	inherited, inForce := ctx.Value(parkExceptKey{}).(map[string]bool)
	except := inherited
	if c.parkAll {
		except = c.parkExcept
		if inForce {
			except = make(map[string]bool, len(c.parkExcept))
			for name := range c.parkExcept {
				if inherited[name] {
					except[name] = true
				}
			}
		}
		ctx = context.WithValue(ctx, parkExceptKey{}, except)
		inForce = true
	}
	if len(c.parkOn) == 0 && len(c.parkSourced) == 0 && !inForce && !c.parkSourceFailed {
		return ctx, nil
	}
	rule := &parkRule{all: inForce, except: except, sourceFailed: c.parkSourceFailed}
	if len(c.parkOn)+len(c.parkSourced) > 0 {
		rule.on = make(map[string]bool, len(c.parkOn)+len(c.parkSourced))
		for _, name := range c.parkOn {
			rule.on[name] = true
		}
		for _, name := range c.parkSourced {
			if !rule.on[name] {
				if rule.sourced == nil {
					rule.sourced = map[string]bool{}
				}
				rule.sourced[name] = true
			}
		}
		for name := range rule.sourced {
			rule.on[name] = true
		}
	}
	return ctx, rule
}

// export returns the rule as data for RunResult.Park: the run's own
// ParkOn names (not the ones a ParkSource supplied) and the
// default-deny half. nil when neither is in force.
func (p *parkRule) export() *ParkRule {
	if p == nil {
		return nil
	}
	out := &ParkRule{All: p.all}
	for name := range p.on {
		if !p.sourced[name] {
			out.On = append(out.On, name)
		}
	}
	if p.all {
		for name := range p.except {
			out.AllExcept = append(out.AllExcept, name)
		}
	}
	if len(out.On) == 0 && !out.All {
		return nil
	}
	slices.Sort(out.On)
	slices.Sort(out.AllExcept)
	return out
}

type onMessagesOption struct {
	fn func(context.Context, int, []Message)
}

func (o onMessagesOption) applyRun(c *runConfig) {
	if o.fn != nil {
		c.onMessages = append(c.onMessages, o.fn)
	}
}

// OnMessages returns the RunOption registering an observer the loop
// calls whenever messages join the run's transcript: the assistant
// message a step produced (reasoning, text and calls in their final
// shape, signatures included), the tool message that follows its calls,
// and the messages a steering drain delivered. msgs is exactly what
// joined, in transcript order, as a deep copy — retaining or mutating
// it changes nothing the run sees — and step is the step the messages
// belong to (a steer's messages name the step whose drain delivered
// them). The calls are synchronous on the run's goroutine, in transcript
// order, so a consumer that appends each batch to durable storage
// persists a mid-run crash's worth of exact transcript; like Tap an
// observer must be fast and must not block (the run waits for it), and
// like Tap a panic in one is contained and counted (TapPanics), never
// breaking the run. Observers cannot change anything — the transcript
// is the run's; behaviour attaches at the two seams. Several OnMessages
// options run in registration order. A Subagent's child run does not
// inherit them: a child's transcript belongs to whoever runs the child
// (the same rule as Steering). This is TODO §5.12's shape (b), the
// answer to "reconstruct messages from events (lossy: signatures, block
// boundaries) or wait for the run to end": the exact bytes, as they
// join.
func OnMessages(fn func(ctx context.Context, step int, msgs []Message)) RunOption {
	return onMessagesOption{fn}
}

type idempotencyKeysOption struct{ fn func(Call) string }

func (o idempotencyKeysOption) applyRun(c *runConfig) {
	if o.fn != nil {
		c.keys = o.fn
	}
}

// IdempotencyKeys returns the RunOption naming each tool call's
// Call.IdempotencyKey. The loop calls fn once per call it is about to
// execute — an ordinary step's and an approved call a resume runs —
// with the Call as the handler will see it (IdempotencyKey still
// empty), before the tool's middleware chain starts; a non-empty
// result is the key the middleware and the handler read, an empty one
// falls back to the run-scoped key (Call.IdempotencyKey). Calls are
// made on the dispatching goroutine, in call order; fn must be fast and
// safe for concurrent use with the run's other observers. weft/thread
// passes it on every turn, so a session's key is recovery-stable: the
// same call keeps its key across a crash, a restart and the resume
// that runs it. A Subagent's child run does not inherit it: its calls
// carry run-scoped keys, so a ReplayKeyed tool inside a Subagent is
// recovery-stable only within one child run. The last option wins
// (ADR 0031, the dispatch seam).
func IdempotencyKeys(fn func(c Call) string) RunOption { return idempotencyKeysOption{fn} }

// DispatchFunc is the gate BeforeDispatch installs: called right
// before a tool call's handler runs, with the Call the handler will
// see (its IdempotencyKey set) and the tool's definition. It answers
// one question — may this call start now? — and does nothing else the
// loop knows of: nil lets the call run; a non-nil error stops it, the
// handler never runs, and the model sees an error result wrapping
// ErrNotDispatched and the gate's error.
type DispatchFunc func(ctx context.Context, c Call, def *ToolDef) error

type dispatchOption struct{ fn DispatchFunc }

func (o dispatchOption) applyRun(c *runConfig) {
	if o.fn != nil {
		c.dispatch = o.fn
	}
}

// BeforeDispatch returns the RunOption installing fn as the run's
// dispatch gate (ADR 0031): the loop calls it for every call about to
// execute — past the agent's and the tool's middleware and the
// approval boundary, so a parked call is never gated, immediately
// before the arguments are decoded and the handler runs — on the
// call's goroutine. It is a gate, not a hook to do work in: fail
// closed when a precondition of starting a side effect does not hold.
// weft/thread's gate refuses a call that is not ReplaySafe unless the
// step entry holding it is durable, so a crash never leaves a side
// effect the session file does not show was issued. A middleware that
// calls next more than once passes the gate once per attempt. The
// gate runs inside the call's Timeout: a call whose Timeout passes (or
// whose run is canceled) before the gate returns is answered as timed
// out (canceled), and its handler never starts, even when the gate
// then lets it through. A Subagent's child run — any run started on
// the call's context — does not inherit it. A CallTool from inside the
// handler passes it again, as an inner call (Call.Outer names the
// dispatching call; ADR 0031 §4); a CallTool from a WrapTools
// middleware, before the call's handler started, is refused with
// ErrNotDispatched under a gate. The last option wins.
func BeforeDispatch(fn DispatchFunc) RunOption { return dispatchOption{fn} }

// AfterDispatchFunc is the hook AfterDispatch installs: called once the
// handler of a call has returned or panicked, with the Call and the
// tool's definition the gate saw.
type AfterDispatchFunc func(ctx context.Context, c Call, def *ToolDef)

type afterDispatchOption struct{ fn AfterDispatchFunc }

func (o afterDispatchOption) applyRun(c *runConfig) {
	if o.fn != nil {
		c.afterDispatch = o.fn
	}
}

// AfterDispatch returns the RunOption installing fn as the run's
// after-dispatch hook (ADR 0031, ADR 0032): the loop calls it on the
// call's goroutine once a handler that started — past the middleware,
// the approval boundary and the BeforeDispatch gate — has returned or
// panicked. It is called also for a handler the loop abandoned at its
// Timeout or at the run's cancellation, when that handler finally
// returns, so a caller can tell whether a call it may retry is still
// running (weft/thread waits for it before a keyed retry). A call the
// gate let through after the loop had given up on it (BeforeDispatch)
// calls it at once: its handler never starts. A handler that never
// returns never calls it. A CallTool from inside the handler
// inherits it; a Subagent's child run — any run started on the call's
// context — does not. The last option wins.
func AfterDispatch(fn AfterDispatchFunc) RunOption { return afterDispatchOption{fn} }

type afterDispatchKey struct{}

func withAfterDispatch(ctx context.Context, fn AfterDispatchFunc) context.Context {
	return context.WithValue(ctx, afterDispatchKey{}, fn)
}

func afterDispatchFrom(ctx context.Context) AfterDispatchFunc {
	fn, _ := ctx.Value(afterDispatchKey{}).(AfterDispatchFunc)
	return fn
}

type parkUnknownOption struct{}

func (parkUnknownOption) applyRun(c *runConfig) { c.parkUnknown = true }

// ParkUnknown returns the RunOption that parks a call whose outcome is
// unknown instead of answering it (ADR 0032): a call of a tool that is
// not ReplaySafe whose handler started and gave no answer of its own —
// its Timeout passed, or the run was canceled under it. Without it the
// call's result is the pinned error "OUTCOME_UNKNOWN: …" (Code
// CodeOutcomeUnknown) the model reads; with it the call lands on
// RunResult.Pending with no result (weft.tool.pending.source
// outcome_unknown), and the run ends at that boundary after its step,
// the other calls' results kept — for a caller that resolves it by a
// ladder (weft/thread: a keyed retry or a person; the tool's Reconcile
// is asked only by RecoverTurn after a crash or a shutdown, never for
// a call parked in a live turn, whose handler may still be running).
// A Subagent's child run — any run started on the call's context — does
// not inherit it. Approving a parked call runs it again in the resume,
// a new run: its default IdempotencyKey is run-scoped and differs
// there, so a caller that retries such a call needs IdempotencyKeys to
// keep its key (weft/thread passes it; Call.IdempotencyKey).
func ParkUnknown() RunOption { return parkUnknownOption{} }

type dispatchKey struct{}

func withDispatch(ctx context.Context, fn DispatchFunc) context.Context {
	return context.WithValue(ctx, dispatchKey{}, fn)
}

func dispatchFrom(ctx context.Context) DispatchFunc {
	fn, _ := ctx.Value(dispatchKey{}).(DispatchFunc)
	return fn
}

// innerKey is the key of the n-th (1-based) call a handler dispatches
// with CallTool, derived from its outer call's key (Call.IdempotencyKey
// documents the format).
func innerKey(outer string, n int64) string {
	h := sha256.Sum256([]byte(outer + "\x00" + strconv.FormatInt(n, 10)))
	return hex.EncodeToString(h[:16])
}

// middlewareKey is the key of the n-th (1-based) call a WrapTools
// middleware dispatches with CallTool under a call of a run with no
// dispatch gate: innerKey's derivation with a "wrap" field between the
// outer key and the position, so it never equals the key of a call the
// handler dispatches (Call.IdempotencyKey documents the format).
func middlewareKey(outer string, n int64) string {
	h := sha256.Sum256([]byte(outer + "\x00wrap\x00" + strconv.FormatInt(n, 10)))
	return hex.EncodeToString(h[:16])
}

// innerDispatch is what a handler's CallTool inherits from the call it
// serves: that call's key and id, the run's dispatch gate, and the
// count of calls it has dispatched so far (their positions).
type innerDispatch struct {
	outer Call
	gate  DispatchFunc
	after AfterDispatchFunc
	n     atomic.Int64
	// early marks the scope the loop installs on the call's context
	// before its tool chain runs: a CallTool there comes from a
	// WrapTools middleware, not from the call's handler (CallTool).
	early bool
	// mark is the outer call's: once the loop settled that call, an
	// inner CallTool is refused (ErrNotDispatched). Nil on the early
	// scope.
	mark *handlerMark
}

type innerKeyCtx struct{}

func withInner(ctx context.Context, in *innerDispatch) context.Context {
	return context.WithValue(ctx, innerKeyCtx{}, in)
}

func innerFrom(ctx context.Context) *innerDispatch {
	in, _ := ctx.Value(innerKeyCtx{}).(*innerDispatch)
	return in
}

// onlyToolsKey carries a run's OnlyTools narrowing on its tool calls'
// contexts, so a CallTool from a handler resolves against the tools the
// run was narrowed to, as the loop's own calls do.
type onlyToolsKey struct{}

func withOnlyTools(ctx context.Context, names []string) context.Context {
	return context.WithValue(ctx, onlyToolsKey{}, names)
}

func onlyToolsFrom(ctx context.Context) []string {
	names, _ := ctx.Value(onlyToolsKey{}).([]string)
	return names
}

// clearCallScope drops what a tool call's context carries for that
// call's own handler and the CallTool calls it makes — the dispatch
// gate and hook, ParkUnknown, the park rule, the OnlyTools narrowing,
// the inner-dispatch keying — so a run started on it inherits none of
// them (the options' "a Subagent's child run does not inherit it",
// ADR 0031 §4). Each is replaced only when present: the common path
// allocates nothing.
func clearCallScope(ctx context.Context) context.Context {
	if dispatchFrom(ctx) != nil {
		ctx = withDispatch(ctx, nil)
	}
	if afterDispatchFrom(ctx) != nil {
		ctx = withAfterDispatch(ctx, nil)
	}
	if parkUnknownFrom(ctx) {
		ctx = context.WithValue(ctx, parkUnknownKey{}, false)
	}
	if parkRuleFrom(ctx) != nil {
		ctx = context.WithValue(ctx, parkRuleKey{}, (*parkRule)(nil))
	}
	if onlyToolsFrom(ctx) != nil {
		ctx = withOnlyTools(ctx, nil)
	}
	if innerFrom(ctx) != nil {
		ctx = withInner(ctx, nil)
	}
	return ctx
}

// defaultIdempotencyKey is the key a call carries when no
// IdempotencyKeys option names one (Call.IdempotencyKey documents the
// format). Stable for one run; a call resumed by another run (an
// approval resume) gets another unless the caller names its own —
// weft/thread does.
func defaultIdempotencyKey(runID, callID string) string {
	h := sha256.Sum256([]byte(runID + "\x00" + callID))
	return hex.EncodeToString(h[:16])
}

// newRunID returns a random 128-bit hex identifier.
func newRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("weft: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func (c *runConfig) finish() {
	if c.id == "" {
		c.id = newRunID()
	}
}

// effectiveThinking resolves the run's reasoning request: a run-level
// Thinking option overrides the agent's construction-time default;
// neither set keeps the provider default (the zero ThinkingConfig).
func (c *runConfig) effectiveThinking(agentDefault ThinkingConfig) ThinkingConfig {
	if c.thinkingSet {
		return c.thinking
	}
	return agentDefault
}

// effectiveToolChoice resolves the run's tool-choice request the same
// way: a run-level ToolChoice option overrides the agent's default;
// neither set keeps the provider default (the zero ToolChoiceConfig).
func (c *runConfig) effectiveToolChoice(agentDefault ToolChoiceConfig) ToolChoiceConfig {
	if c.toolChoiceSet {
		return c.toolChoice
	}
	return agentDefault
}

// effectiveParams resolves the run's sampling request the same way: a
// run-level Params option replaces the agent's default whole (no field
// merge — the Thinking rule); neither set keeps the adapter's
// construction defaults (the zero RequestParams).
func (c *runConfig) effectiveParams(agentDefault RequestParams) RequestParams {
	if c.paramsSet {
		return c.params
	}
	return agentDefault
}

// effectiveSystem resolves the run's system prompt: a run-level
// Instructions option replaces the agent's; neither set keeps the
// agent's (the Thinking rule).
func (c *runConfig) effectiveSystem(agentDefault string) string {
	if c.systemSet {
		return c.system
	}
	return agentDefault
}

// effectiveMaxSteps resolves the run's step budget. The caller has
// already validated the lower-only rule; this only picks the value.
func (c *runConfig) effectiveMaxSteps(agentDefault int) int {
	if c.maxStepsSet {
		return c.maxSteps
	}
	return agentDefault
}

// effectiveParallelism resolves the run's tool width, the same rule.
func (c *runConfig) effectiveParallelism(agentDefault int) int {
	if c.parallelismSet {
		return c.parallelism
	}
	return agentDefault
}

type promptOption string

func (o promptOption) applyRun(c *runConfig) {
	c.messages = append(c.messages, User(string(o)))
}

// Prompt adds a user message to the run's input.
func Prompt(text string) RunOption { return promptOption(text) }

type messagesOption []Message

func (o messagesOption) applyRun(c *runConfig) {
	c.messages = append(c.messages, o...)
}

// Messages adds existing messages (a session transcript, few-shot examples)
// to the run's input.
func Messages(msgs ...Message) RunOption { return messagesOption(msgs) }

// StepRecord captures everything one model step produced: its text, the
// tool calls it requested, and the results of executing them in call order.
type StepRecord struct {
	Index int
	// StopReason is the mapped reason (stop, tool_calls, max_tokens);
	// RawStopReason is the provider's own value when the mapping was
	// approximated ("refusal", "content_filter", ...) — see
	// ModelFinish.Raw.
	StopReason    StopReason
	RawStopReason string
	Usage         Usage
	Text          string
	ToolCalls     []ToolCallPart
	Results       []ToolResultPart
	// SubagentUsage is the usage of each child run this step started,
	// keyed by the parent's call id — including a failed child's partial
	// usage. It is already included in RunResult.Usage; Usage above is
	// the step's own model call only. Nil when the step ran no subagent.
	SubagentUsage map[string]Usage
}

// RunResult is the outcome of a completed run: its id, the full transcript
// (including the input messages), one record per step, and summed usage.
type RunResult struct {
	ID string
	// StopReason is the last step's finish reason. StopMaxTokens here
	// means the final reply was cut off by the output-token limit — the
	// run still succeeds, and the caller decides what truncated text
	// means.
	StopReason StopReason
	Messages   []Message
	Steps      []StepRecord
	Usage      Usage
	// Pending lists the tool calls of the last step that await an
	// approval decision (RequireApproval, or middleware returning
	// ErrApprovalRequired). The run ended successfully without running
	// them and the transcript carries no result for them; resume with
	// Messages(res.Messages...) plus Approve/Deny per call.
	Pending []ToolCallPart
	// Park is the park rule the run ran under — its own ParkOn and
	// ParkAllExcept, and a ParkAllExcept it inherited from the run it
	// was started inside — nil when no rule was in force. A caller that
	// persists a parked run keeps it beside the pending calls and puts
	// it back on the resuming run (ParkRule.Options), so a resume after
	// a restart parks what the parked run would have parked. The names
	// an agent's ParkSource supplied are not in it: the resuming run
	// reads its sources again.
	Park *ParkRule
}

// ParkRule is a run's park rule as data (RunResult.Park): the tools the
// run's ParkOn named, and, when All, the default-deny half — every
// call parks except one to a tool AllExcept names. Names are sorted.
type ParkRule struct {
	On        []string `json:"on,omitempty"`
	All       bool     `json:"all,omitempty"`
	AllExcept []string `json:"all_except,omitempty"`
}

// Options returns the run options that put the rule back on a run:
// ParkOn for On, and ParkAllExcept for the default-deny half. A nil
// rule returns none.
func (p *ParkRule) Options() []RunOption {
	if p == nil {
		return nil
	}
	var opts []RunOption
	if len(p.On) > 0 {
		opts = append(opts, ParkOn(p.On...))
	}
	if p.All {
		opts = append(opts, ParkAllExcept(p.AllExcept...))
	}
	return opts
}

// RunSettings is what a run is started under, as data: the run options
// that can be written down, and the values its context carries for it
// (metadata placed with WithMetadata or inherited from a delegating
// run, and an inherited ParkAllExcept). A caller that persists a run
// to start or resume later — in another process, after a restart —
// records it (SettingsOf) and puts it back on that run (Options), so
// the run is narrowed, budgeted and tagged as the one it stands for.
//
// What is not data is not in it: UseModel (a Model is code),
// OnMessages and Steering (functions), and the options that name the
// run itself (Messages, Prompt, RunID, ResumeOf, KeptSteps) or decide
// its calls (Approve, Deny, Resolve). A restored run runs without them.
type RunSettings struct {
	// Park is the park rule (ParkOn and ParkAllExcept, own or
	// inherited), nil when none is in force.
	Park *ParkRule `json:"park,omitempty"`
	// OnlyTools is the OnlyTools narrowing, sorted; nil keeps the
	// agent's full set.
	OnlyTools []string `json:"only_tools,omitempty"`
	// MaxSteps and Parallelism are the run-level values of the dual
	// options (lower only); 0 when the run set none.
	MaxSteps    int `json:"max_steps,omitempty"`
	Parallelism int `json:"parallelism,omitempty"`
	// Instructions is a run-level Instructions override, nil when the
	// run keeps the agent's prompt.
	Instructions *string `json:"instructions,omitempty"`
	// Thinking, ToolChoice and Params are the run-level overrides of
	// the dual options, nil when the run set none.
	Thinking   *ThinkingConfig   `json:"thinking,omitempty"`
	ToolChoice *ToolChoiceConfig `json:"tool_choice,omitempty"`
	Params     *RequestParams    `json:"params,omitempty"`
	// Metadata is the caller pairs the run carries: the context's
	// merged under the run's own Metadata options. An empty value is
	// kept — it removes the key on the restored run, as it did on the
	// original.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// SettingsOf returns the settings a run started under ctx with opts
// would run under, as data (RunSettings) — nil when there are none.
// Options that are not data are skipped (RunSettings says which).
func SettingsOf(ctx context.Context, opts ...RunOption) *RunSettings {
	var c runConfig
	for _, o := range opts {
		if o != nil {
			o.applyRun(&c)
		}
	}
	_, rule := c.resolvePark(ctx)
	s := RunSettings{Park: rule.export()}
	if len(c.onlyTools) > 0 {
		s.OnlyTools = nameSet(c.onlyTools)
	}
	if c.maxStepsSet {
		s.MaxSteps = c.maxSteps
	}
	if c.parallelismSet {
		s.Parallelism = c.parallelism
	}
	if c.systemSet {
		text := c.system
		s.Instructions = &text
	}
	if c.thinkingSet {
		t := c.thinking
		s.Thinking = &t
	}
	if c.toolChoiceSet {
		tc := c.toolChoice
		s.ToolChoice = &tc
	}
	if c.paramsSet {
		p := cloneParams(c.params)
		s.Params = &p
	}
	if md := metadataFromCtx(ctx); len(md) > 0 || len(c.metadata) > 0 {
		s.Metadata = maps.Clone(md)
		if s.Metadata == nil {
			s.Metadata = make(map[string]string, len(c.metadata))
		}
		maps.Copy(s.Metadata, c.metadata)
	}
	if s.Park == nil && s.OnlyTools == nil && s.MaxSteps == 0 && s.Parallelism == 0 &&
		s.Instructions == nil && s.Thinking == nil && s.ToolChoice == nil && s.Params == nil &&
		s.Metadata == nil {
		return nil
	}
	return &s
}

// Options returns the run options that put the settings back on a run.
// A nil RunSettings returns none. The options obey their usual rules on
// the run they are given to: an OnlyTools name the agent no longer
// registers, or a MaxSteps or Parallelism above the agent's, fails that
// run with ErrInvalidRunOption before any model call — a restored
// narrowing is never dropped to let the run start.
func (s *RunSettings) Options() []RunOption {
	if s == nil {
		return nil
	}
	opts := s.Park.Options()
	if len(s.OnlyTools) > 0 {
		opts = append(opts, OnlyTools(s.OnlyTools...))
	}
	if s.MaxSteps > 0 {
		opts = append(opts, MaxSteps(s.MaxSteps))
	}
	if s.Parallelism > 0 {
		opts = append(opts, Parallelism(s.Parallelism))
	}
	if s.Instructions != nil {
		opts = append(opts, Instructions(*s.Instructions))
	}
	if s.Thinking != nil {
		opts = append(opts, Thinking(*s.Thinking))
	}
	if s.ToolChoice != nil {
		opts = append(opts, ToolChoice(*s.ToolChoice))
	}
	if s.Params != nil {
		opts = append(opts, Params(cloneParams(*s.Params)))
	}
	if len(s.Metadata) > 0 {
		opts = append(opts, Metadata(maps.Clone(s.Metadata)))
	}
	return opts
}

// Clone returns a deep copy of s; nil for nil.
func (s *RunSettings) Clone() *RunSettings {
	if s == nil {
		return nil
	}
	out := *s
	if s.Park != nil {
		p := *s.Park
		p.On, p.AllExcept = slices.Clone(p.On), slices.Clone(p.AllExcept)
		out.Park = &p
	}
	out.OnlyTools = slices.Clone(s.OnlyTools)
	if s.Instructions != nil {
		text := *s.Instructions
		out.Instructions = &text
	}
	if s.Thinking != nil {
		t := *s.Thinking
		out.Thinking = &t
	}
	if s.ToolChoice != nil {
		tc := *s.ToolChoice
		out.ToolChoice = &tc
	}
	if s.Params != nil {
		p := cloneParams(*s.Params)
		out.Params = &p
	}
	out.Metadata = maps.Clone(s.Metadata)
	return &out
}

// NumSteps returns how many model calls the run made.
func (r *RunResult) NumSteps() int { return len(r.Steps) }

// Text returns the final assistant text — the text parts of the last
// assistant message.
func (r *RunResult) Text() string {
	for i := len(r.Messages) - 1; i >= 0; i-- {
		if r.Messages[i].Role == RoleAssistant {
			return r.Messages[i].Text()
		}
	}
	return ""
}

// Run is a handle to one streaming execution. Create it with Agent.Stream,
// then either range over Events (exactly once) or call Wait, which runs the
// agent to completion and reports the final result.
type Run struct {
	agent   *Agent
	ctx     context.Context
	cancel  context.CancelFunc
	cfg     runConfig
	mu      sync.Mutex
	started bool
	result  *RunResult
	err     error
	done    chan struct{}
}

// Stream starts a run and returns its handle. The run is lazy: nothing
// executes until Events is consumed. Canceling ctx aborts the model call
// and any in-flight tools.
func (a *Agent) Stream(ctx context.Context, opts ...RunOption) *Run {
	cfg := runConfig{}
	for _, o := range opts {
		if o != nil {
			o.applyRun(&cfg)
		}
	}
	cfg.finish()
	ctx, cancel := context.WithCancel(ctx)
	return &Run{agent: a, ctx: ctx, cancel: cancel, cfg: cfg, done: make(chan struct{})}
}

// ID returns the run's identifier. It is fixed at Stream time, so it can be
// logged or handed to a client before the first event is consumed.
func (r *Run) ID() string { return r.cfg.id }

// Events returns the run's event stream. It is single-use; a second call
// yields only ErrRunConsumed.
//
// Events arrive in emission order (see ToolStart for the concurrent-tool
// ordering rule). Delivery is a direct hand-off over an unbuffered
// channel, and tool events are emitted while holding the step's
// event-ordering lock — so a slow consumer does not merely receive late:
// it delays event emission and gates the start of the step's subsequent
// tools. For latency-sensitive parallel tools, consume promptly (range
// over Events in a dedicated goroutine that buffers) or use Generate,
// which needs no consumer.
//
// A failed run delivers its error exactly once as the final
// element; a successful run ends with RunFinish. Breaking out of the range
// cancels the run.
func (r *Run) Events() iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		r.mu.Lock()
		if r.started {
			r.mu.Unlock()
			yield(nil, ErrRunConsumed)
			return
		}
		r.started = true
		r.mu.Unlock()

		ch := make(chan Event)
		go func() {
			// Defers run LIFO: close(ch) ends the consumer's range, then
			// close(done) unblocks Wait, then the run's context is
			// released.
			defer r.cancel()
			defer close(r.done)
			defer close(ch)
			emit := func(ev Event) {
				// The post-cancellation drop lives in execute's emit
				// wrapper (one home for the rule); this select is what
				// prevents blocking when the consumer has gone.
				select {
				case ch <- ev:
				case <-r.ctx.Done():
				}
			}
			res, err := r.agent.execute(r.ctx, r.cfg, emit)
			r.mu.Lock()
			r.result, r.err = res, err
			r.mu.Unlock()
		}()

		for ev := range ch {
			if !yield(ev, nil) {
				r.cancel() // consumer stopped early; unblock the producer
				return
			}
		}
		<-r.done
		r.mu.Lock()
		err := r.err
		r.mu.Unlock()
		if err != nil {
			yield(nil, err)
		}
	}
}

// Close releases the run's resources, canceling it if still running.
// It is for abandoned runs: a run you will consume needs no Close —
// Events and Wait release everything themselves. Safe to call any
// number of times, before or after consumption.
func (r *Run) Close() { r.cancel() }

// Wait blocks until the run finishes and returns its result. If Events has
// not been consumed, Wait runs the agent itself, discarding events; it is
// safe to call after ranging over Events, or from another goroutine while
// ranging over them.
func (r *Run) Wait() (*RunResult, error) {
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		for range r.Events() {
		}
	}
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.result, r.err
}

// Generate runs the agent to completion and returns the final result.
// Internally it is Stream with the events folded away; errors are returned
// as *RunError, with the partial transcript attached.
func (a *Agent) Generate(ctx context.Context, opts ...RunOption) (*RunResult, error) {
	cfg := runConfig{}
	for _, o := range opts {
		if o != nil {
			o.applyRun(&cfg)
		}
	}
	cfg.finish()
	return a.execute(ctx, cfg, func(Event) {})
}

// The weft.override.* span attributes (ADR 0024 S1.2, WEFT-PLAYGROUND
// §10.1): one per knob a RunOption changed, plus the hash over their
// canonical JSON — an experiment's fingerprint, so two sibling runs
// with equal changes carry equal hashes and a plain run carries none.
const (
	attrOverrideHash         = "weft.override.hash"
	attrOverrideInstructions = "weft.override.instructions"
	attrOverrideTools        = "weft.override.tools"
	attrOverrideModel        = "weft.override.model"
	attrOverrideParkOn       = "weft.override.park_on"
	attrOverrideParkExcept   = "weft.override.park_all_except"
	attrOverrideThinking     = "weft.override.thinking"
	attrOverrideToolChoice   = "weft.override.tool_choice"
	attrOverrideParams       = "weft.override.params"
	attrOverrideMaxSteps     = "weft.override.max_steps"
	attrOverrideParallelism  = "weft.override.parallelism"
)

// overrideAttrs renders the run's weft.override.* attributes: present
// only when a RunOption changed the agent's configuration, absent on a
// plain run. "Changed" means a run-level option was applied — the value
// may coincide with the agent's own; the fingerprint records what the
// run carried, not a diff. The instructions text itself is content: the
// attribute says only that it was replaced (true), and spans carry no
// text (ADR 0016 O7). The text is recorded under the content policy by
// the prompt record (ADR 0028, which reversed the earlier stance that
// no record carries it). The hash covers every
// changed value, text included, as sha256 over the canonical JSON of a
// map (encoding/json sorts map keys, so equal changes hash equal); the
// tool subset and the parked tools enter it as sets (nameSet).
func (c *runConfig) overrideAttrs(agentParams RequestParams) []attribute.KeyValue {
	values := map[string]any{}
	var attrs []attribute.KeyValue
	add := func(name string, attrValue any, kv attribute.KeyValue) {
		values[name] = attrValue
		attrs = append(attrs, kv)
	}
	if c.systemSet {
		add("instructions", c.system, attribute.Bool(attrOverrideInstructions, true))
	}
	if len(c.onlyTools) > 0 {
		names := nameSet(c.onlyTools)
		add("tools", names, attribute.String(attrOverrideTools, strings.Join(names, ",")))
	}
	if c.model != nil {
		info := InfoOf(c.model)
		name := info.Name
		if info.Provider != "" {
			name = info.Provider + "/" + info.Name
		}
		add("model", name, attribute.String(attrOverrideModel, name))
	}
	if c.thinkingSet {
		add("thinking", thinkingOverride(c.thinking), attribute.String(attrOverrideThinking, thinkingOverride(c.thinking)))
	}
	if c.toolChoiceSet {
		tc := c.toolChoice
		value := string(tc.Mode)
		switch tc.Mode {
		case ToolChoiceAuto:
			// The zero mode is auto: spelled out, so an auto override
			// over a forcing default reads "auto", never "".
			value = "auto"
		case ToolChoiceNamed:
			// A named choice with no name (refused at the model call)
			// reads as its mode, never "tool:".
			if tc.Name != "" {
				value = string(ToolChoiceNamed) + ":" + tc.Name
			}
		}
		add("tool_choice", value, attribute.String(attrOverrideToolChoice, value))
	}
	if c.paramsSet {
		// The set knobs by the request record's lower-case names
		// (UX-S5b-03), and — since a Params option replaces the agent's
		// struct whole (effectiveParams) — each knob the agent set that
		// the replacement leaves unset, as null: {"temperature":0.4,
		// "top_p":null} over an agent with TopP, never a one-knob merge.
		// The hash takes the set knobs alone, so equal overrides hash
		// equal whatever the agent's defaults.
		set, err := json.Marshal(paramsBody{
			Temperature: c.params.Temperature,
			TopP:        c.params.TopP,
			MaxTokens:   c.params.MaxTokens,
			Stop:        c.params.Stop,
			Seed:        c.params.Seed,
		})
		if err != nil {
			set = []byte("{}")
		}
		add("params", json.RawMessage(set), attribute.String(attrOverrideParams, paramsReplacement(c.params, agentParams)))
	}
	if c.maxStepsSet {
		add("max_steps", c.maxSteps, attribute.Int(attrOverrideMaxSteps, c.maxSteps))
	}
	if c.parallelismSet {
		add("parallelism", c.parallelism, attribute.Int(attrOverrideParallelism, c.parallelism))
	}
	if len(c.parkOn) > 0 {
		names := nameSet(c.parkOn)
		add("park_on", names, attribute.String(attrOverrideParkOn, strings.Join(names, ",")))
	}
	if c.parkAll {
		// The run's own except-list — a set, on its own attribute, so
		// park_on keeps meaning ParkOn. Present and empty when the run
		// parks everything.
		names := slices.Sorted(maps.Keys(c.parkExcept))
		add("park_all_except", names, attribute.String(attrOverrideParkExcept, strings.Join(names, ",")))
	}
	if len(values) == 0 {
		return nil
	}
	b, err := json.Marshal(values)
	if err != nil {
		return attrs // unhashable overrides still name themselves
	}
	sum := sha256.Sum256(b)
	return append([]attribute.KeyValue{attribute.String(attrOverrideHash, hex.EncodeToString(sum[:]))}, attrs...)
}

// paramsReplacement renders weft.override.params: the replacing
// struct's set knobs in the request record's order and names, with
// null for each knob the agent's default set and the replacement
// leaves unset — what the run lost by the whole-struct replace.
func paramsReplacement(run, agent RequestParams) string {
	var b strings.Builder
	b.WriteByte('{')
	knob := func(name string, set, agentSet bool, v any) {
		if !set && !agentSet {
			return
		}
		if b.Len() > 1 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(name) + ":")
		if !set {
			b.WriteString("null")
			return
		}
		j, err := json.Marshal(v)
		if err != nil {
			j = []byte("null")
		}
		b.Write(j)
	}
	knob("temperature", run.Temperature != nil, agent.Temperature != nil, run.Temperature)
	knob("top_p", run.TopP != nil, agent.TopP != nil, run.TopP)
	knob("max_tokens", run.MaxTokens != nil, agent.MaxTokens != nil, run.MaxTokens)
	knob("stop", len(run.Stop) > 0, len(agent.Stop) > 0, run.Stop)
	knob("seed", run.Seed != nil, agent.Seed != nil, run.Seed)
	b.WriteByte('}')
	return b.String()
}

// nameSet renders an option's accumulated tool names as the set they
// are — sorted, each once — so the fingerprint and its attribute do not
// depend on the order the names were given in, or on a name several
// options repeated.
func nameSet(names []string) []string {
	out := slices.Clone(names)
	slices.Sort(out)
	return slices.Compact(out)
}

// thinkingOverride renders a ThinkingConfig for weft.override.thinking:
// the level by name, the budget beside it when set.
func thinkingOverride(cfg ThinkingConfig) string {
	level := thinkingLevelName(cfg.Level)
	if cfg.Budget > 0 {
		return level + "/" + strconv.FormatInt(cfg.Budget, 10)
	}
	return level
}
