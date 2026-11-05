package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/weftbase/weft/core"
	"github.com/weftbase/weft/core/wefttest"
)

// ExampleAgent_ServeCall serves one tool call from outside any run — a
// request a server received, with the client's own idempotency key —
// under the tool's Timeout and result cap, as the loop would run it.
func ExampleAgent_ServeCall() {
	charge := core.Tool("charge", "Charge a card.", func(ctx context.Context, _ struct{}) (string, error) {
		c, _ := core.CallFromContext(ctx)
		return "charged under key " + c.IdempotencyKey, nil // pass the key to the payment API
	}, core.Timeout(5*time.Second), core.Replay(core.ReplayKeyed))
	agt := core.New(wefttest.Script(wefttest.Say("unused")), charge)
	out, err := agt.ServeCall(context.Background(),
		core.ToolCallPart{ID: "req-7", Name: "charge", Args: json.RawMessage(`{}`)}, "client-key-7")
	fmt.Println(out, err)
	// Output: charged under key client-key-7 <nil>
}
