package anthropic

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/weftbase/weft/core"
)

// sseModel answers every request with body as an event stream.
func sseModel(t *testing.T, body string) core.Model {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	c := testClient(srv)
	return Model("m", Client(&c))
}

const sseMessageStart = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n"

// A gap in the content-block indexes (a gateway that numbers from one,
// or strips the block at index 0) does not hide the tool calls after
// it: a tool_use stop with no call is a call that vanished.
func TestContentBlockIndexGapKeepsTheToolCall(t *testing.T) {
	body := sseMessageStart +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"lookup\",\"input\":{}}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":1}\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":3,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_2\",\"name\":\"lookup\",\"input\":{}}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":3}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":5}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	evs, err := collect(sseModel(t, body), basicReq)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, ev := range evs {
		if c, ok := ev.(core.ModelToolCall); ok {
			ids = append(ids, c.ID)
		}
	}
	if len(ids) != 2 || ids[0] != "toolu_1" || ids[1] != "toolu_2" {
		t.Fatalf("tool calls = %v, want toolu_1 then toolu_2 (finish %+v)", ids, lastFinish(t, evs))
	}
}

// Rule 8: a response body that ends cleanly before a stop reason or
// message_stop is a truncated answer — an error, never an end_turn
// finish over the partial text.
func TestStreamEndingBeforeMessageStopIsAnError(t *testing.T) {
	body := sseMessageStart +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"The answer is\"}}\n\n"
	evs, err := collect(sseModel(t, body), basicReq)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated stream: err = %v, want io.ErrUnexpectedEOF", err)
	}
	for _, ev := range evs {
		if f, ok := ev.(core.ModelFinish); ok {
			t.Fatalf("a truncated stream yielded a finish: %+v", f)
		}
	}
}
