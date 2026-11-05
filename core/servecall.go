package core

import "context"

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
// (ADR 0032). A RequireApproval tool, or a middleware's park, returns
// an error wrapping ErrApprovalRequired: there is no run to park, so
// the call does not run. Like CallTool it produces no span, event or
// log line.
func (a *Agent) ServeCall(ctx context.Context, call ToolCallPart, key string) (string, error) {
	tools, err := a.dispatchTools()
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
	fn := a.chain(def, strict, parkRuleFrom(ctx), mark, nil)
	var out string
	var fail toolFailure
	if timeout <= 0 {
		out, fail = invokeContained(ctx, fn, call, mark, nil)
	} else {
		out, fail = invokeWithTimeout(ctx, fn, call, timeout, mark)
	}
	if fail.err == nil {
		out, _ = capResultN(out, resultCap)
		return out, nil
	}
	if fail.park {
		return "", fail.err
	}
	text, unknown := outcomeUnknown(ctx, call, def, fail, mark, timeout)
	if !unknown {
		text = fail.text
	}
	text, _ = capResultN(text, resultCap)
	return "", &servedCallError{text: text, err: fail.err, unknown: unknown}
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
