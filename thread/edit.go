package thread

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"

	"github.com/weftbase/weft/core"
)

// canonicalArgs is the one form edited arguments are hashed, signed,
// recorded and run in: decoded (numbers kept as written) and encoded
// again — object keys sorted, no insignificant space, no HTML escaping.
// It is stable across the escaping a JSON encoder applies on the way to
// a file or a wire (json.Marshal escapes "<" in a json.RawMessage), so
// the hash of what was signed is the hash of what is read back. Invalid
// JSON is an error.
func canonicalArgs(b []byte) (json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("thread: arguments hold more than one JSON value")
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(out.Bytes(), "\n"), nil
}

// canonicalHash is the SHA-256 of b's canonical form, "" for bytes
// that are not JSON.
func canonicalHash(b []byte) string {
	c, err := canonicalArgs(b)
	if err != nil {
		return ""
	}
	return hashArgs(c)
}

// editedHash is the hash an edit decision records for its edited
// arguments, "" for every other decision.
func editedHash(d Decision) string {
	if d.Kind != OutcomeEdit {
		return ""
	}
	return canonicalHash(d.EditedArgs)
}

// jsonObject reports whether b is a JSON object.
func jsonObject(b json.RawMessage) bool {
	var m map[string]json.RawMessage
	return len(b) > 0 && json.Unmarshal(b, &m) == nil && m != nil
}

// editedArgsOf reports whether the call runs on edited arguments — its
// request is an edit's revision, or a recovery retry of one (Args that
// are not the model's) — and returns them canonical, with their hash.
// ok is false when a revision's arguments no longer hash to what it
// recorded: the call must not run on them.
func editedArgsOf(re ApprovalRequestEntry, c core.ToolCallPart) (args json.RawMessage, sha string, edited, ok bool) {
	if re.RevisionOf == "" && (!re.Recovery || len(re.Args) == 0 || canonicalHash(re.Args) == canonicalHash(c.Args)) {
		return nil, "", false, true
	}
	args, err := canonicalArgs(re.Args)
	if err != nil {
		return nil, "", true, false
	}
	sha = hashArgs(args)
	if re.RevisionOf != "" && sha != re.ArgsSHA256 {
		return nil, "", true, false
	}
	return slices.Clone(args), sha, true, true
}
