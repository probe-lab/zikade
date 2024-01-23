package coord

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/zikade/tele"
)

// Telemetry is the struct that holds a reference to all metrics and the tracer used
// by the coordinator and its components.
// Make sure to also register the [MeterProviderOpts] with your custom or the global
// [metric.MeterProvider].
type Telemetry struct {
	Tracer trace.Tracer
	// TODO: define metrics produced by coordinator
}

// NewTelemetry initializes a Telemetry struct with the given meter and tracer providers.
func NewTelemetry(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (*Telemetry, error) {
	t := &Telemetry{
		Tracer: tracerProvider.Tracer(tele.TracerName),
	}

	// TODO: Initalize metrics produced by the coordinator

	return t, nil
}
