package otel_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/weftbase/weft/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// failingLogs is an exporter whose every export fails.
type failingLogs struct{}

func (failingLogs) Export(context.Context, []sdklog.Record) error {
	return errors.New("collector down")
}
func (failingLogs) Shutdown(context.Context) error   { return nil }
func (failingLogs) ForceFlush(context.Context) error { return nil }

// ExampleErrRecordsLost: Shutdown says when records were lost during
// the pipeline's life — never silent.
func ExampleErrRecordsLost() {
	p, err := otel.Start(context.Background(), otel.NoGlobal(), otel.NoEnv(), otel.Heartbeat(0),
		otel.Exporters(nil, failingLogs{}))
	if err != nil {
		panic(err)
	}
	var rec otellog.Record
	rec.SetBody(attribute.StringValue("hello"))
	p.LoggerProvider().Logger("example").Emit(context.Background(), rec)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = p.Shutdown(ctx)
	var loss *otel.LossError
	if errors.As(err, &loss) {
		fmt.Println(errors.Is(err, otel.ErrRecordsLost), loss.Lost, loss.ByDestination["exporters"])
	}
	// Output: true 1 1
}
