package thread

// bound binds d to the pending request it answers (Decision.Bind), as
// Decide requires: the session's pending request for its call. A call
// that is not pending gets a request id no request carries, so Decide
// answers it with ErrNotPending. A decision that names its request
// already is left as it is.
func bound(s *Session, d Decision) Decision {
	if d.RequestID != "" {
		if d.ArgsSHA256 == "" {
			for _, r := range s.Pending() {
				if r.ID == d.RequestID {
					d.ArgsSHA256 = r.ArgsSHA256
				}
			}
			if d.ArgsSHA256 == "" {
				d.ArgsSHA256 = "0" // no such pending request: ErrNotPending
			}
		}
		return d
	}
	for _, r := range s.Pending() {
		if r.CallID == d.CallID {
			shown := d.ArgsSHA256
			d = d.Bind(r)
			if shown != "" {
				d.ArgsSHA256 = shown // the arguments the test says were shown
			}
			return d
		}
	}
	d.RequestID, d.ArgsSHA256 = "e_not_pending", "0"
	return d
}
