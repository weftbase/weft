package core

import (
	"context"
	"fmt"
	"maps"
)

// ServeCall executes one tool call the way the loop executes a call it
// dispatches, for a server answering calls that come from outside any
// run (weft/mcp's Serve): through the agent's WrapTools chain and the
// tool's own, under the tool's Timeout (else the agent's), with the
// result cap applied last to every outcome (the tool's MaxResultBytes,
// else the agent's), panics contained — the run policy CallTool leaves
// to its caller.
//
// The handler's CallFromContext reports a Call with call's id and name
// and key as its IdempotencyKey; no RunID, no Step, never Approved.
// key is the client's own identity for the call — the one a retried
// request carries again — so a deduplicating handler applies a retry
// once; pass "" when the client sent none (never a random key: a key
// that changes on a retry dedupes nothing, and one shared by two
// distinct calls loses the second).
//
// The returned error's text is the result the loop's model would read:
// a handler error, a contained panic, "tool X timed out after d", the
// result cap's marker on a long error. A tool that is not ReplaySafe
// whose handler started and gave no answer of its own — its Timeout
// passed, ctx ended under it, it panicked — answers with the pinned
// "OUTCOME_UNKNOWN: …" text and an error matching ErrOutcomeUnknown
// (ADR 0032). A RequireApproval tool, a tool the agent's ParkSource
// names (read once per served call, as a run reads it once at its
// start; a source that panicked parks every call but the agent's
// Output answer, failing closed as in a run — ADR 0030), or a
// middleware's park before the handler ran, returns an error wrapping
// ErrApprovalRequired: there is no run to park, so the call does not
// run. A middleware's park after a
// handler that is not ReplaySafe ran (and did not fail on its own) is
// that unknown outcome instead, as in a run: the call did run. A
// ToolSource that panics is the call's error, never a crash of the
// serving process. Like CallTool it produces no span, event or log
// line.
func (a *Agent) ServeCall(ctx context.Context, call ToolCallPart, key string) (string, error) {
	tools, err := a.servedTools()
	if err != nil {
		return "", err
	}
	def, _ := findTool(tools, call.Name)
	resultCap, timeout, strict := a.resultCap, a.toolTimeout, a.strict
	if def != nil {
		if def.capSet {
			resultCap = def.resultCap
		}
		if def.timeoutSet {
			timeout = def.timeout
		}
		strict = strict || def.strict
	}
	ctx = withCall(ctx, Call{CallID: call.ID, Name: call.Name, IdempotencyKey: key})
	mark := new(handlerMark)
	fn := a.chain(def, strict, a.servedParkRule(ctx), mark, nil)
	var out string
	var fail toolFailure
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
	if fail.err == nil {
		out, _ = capResultN(out, resultCap)
		return out, nil
	}
	if fail.park && !ranPastHandler(def, mark) {
		return "", fail.err
	}
	text, unknown := outcomeUnknown(ctx, call, def, fail, mark, timeout)
	if !unknown {
		text = fail.text
	}
	text, _ = capResultN(text, resultCap)
	return "", &servedCallError{text: text, err: fail.err, unknown: unknown}
}

// servedParkRule is the park rule a served call runs under: the one on
// ctx (a ServeCall made inside a run's tool call), merged with the
// agent's ParkSources, read once for this call as a run reads them once
// at its start (ADR 0030). A source that panicked leaves its set
// unknown, so every call parks but the agent's own Output answer —
// fail closed, exactly as in a run. ctx's rule is never mutated.
func (a *Agent) servedParkRule(ctx context.Context) *parkRule {
	base := parkRuleFrom(ctx)
	if len(a.parkSources) == 0 {
		return base
	}
	names, failed := a.readParkSources(ctx, "")
	if len(names) == 0 && !failed {
		return base
	}
	rule := &parkRule{sourceFailed: failed}
	if base != nil {
		rule.all, rule.except = base.all, base.except
		rule.sourceFailed = rule.sourceFailed || base.sourceFailed
		rule.on = maps.Clone(base.on)
		rule.sourced = maps.Clone(base.sourced)
	}
	for _, name := range names {
		if rule.on[name] {
			continue
		}
		if rule.on == nil {
			rule.on = map[string]bool{}
		}
		if rule.sourced == nil {
			rule.sourced = map[string]bool{}
		}
		rule.on[name], rule.sourced[name] = true, true
	}
	return rule
}

// servedTools is dispatchTools contained: a ToolSource that panics
// fails the served call with an error naming the panic — a server
// answering calls from outside any run has no run to fail, and its
// process must outlive one bad registry read.
func (a *Agent) servedTools() (tools []*ToolDef, err error) {
	defer func() {
		if p := recover(); p != nil {
			tools, err = nil, fmt.Errorf("weft: the agent's tool source panicked: %v", p)
		}
	}()
	return a.dispatchTools()
}

// servedCallError is a ServeCall failure: its text is the result the
// loop's model would read (capped), its cause the chain's error, and an
// unknown outcome matches ErrOutcomeUnknown.
type servedCallError struct {
	text    string
	err     error
	unknown bool
}

func (e *servedCallError) Error() string { return e.text }

func (e *servedCallError) Unwrap() []error {
	if e.unknown {
		return []error{ErrOutcomeUnknown, e.err}
	}
	return []error{e.err}
}
