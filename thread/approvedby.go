package thread

import "context"

// WithApprovedBy returns ctx carrying the approval class a layer above
// thread names when it decides through the session on someone's
// behalf: a platform relaying a decision made in its inbox, a devtools
// playground, an integration such as a shop's back office. Pass the
// returned context to Decide (or Resume, under AutoResume(false)):
// every call that decision approves is reported with
// weft.tool.approved_by set to class on its resumed execute_tool span,
// where a plain Decide reports "user" (record contract §3). weftbase/runtime's
// playground passes "runtime".
//
// class is a short id or enum ("runtime", "inbox", "shop"), never who
// by name: the person or system that decided belongs on the decision
// itself (Decision.Who, Decision.Via), which the session records. ""
// clears a class an outer context carried.
//
// It is reporting only: the decision, its validation and its record are
// the session's, unchanged. The class is the arming call's alone — a
// later boundary of the same session decided by a plain Decide reads
// "user" again, though its parked turn ran under this context. Grants,
// the WithApprover step and quorums keep their own classes ("grant",
// "approver", "quorum"); only an approval Decide would report as
// "user" takes class.
func WithApprovedBy(ctx context.Context, class string) context.Context {
	return context.WithValue(ctx, approvedByKey{}, class)
}

// approvedByKey keys the class WithApprovedBy puts on a context.
type approvedByKey struct{}

// approvedByOf returns the class ctx carries (WithApprovedBy), "" when
// none. thread reads it from the context that armed a resume
// (Session.Decide's, Resume's), never from the parked turn's.
func approvedByOf(ctx context.Context) string {
	by, _ := ctx.Value(approvedByKey{}).(string)
	return by
}
