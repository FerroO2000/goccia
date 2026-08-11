# Metrics Generator: typed error metrics

Status: design handoff for a future session. The JSON metrics exist, but
successful operations currently export `error.type=""`. The work below has not
been implemented yet.

## Objective

Teach `metrics-gen` that errors are a special kind of metric attribute:

- a successful operation must omit `error.type`;
- a failed operation must include one predictable, low-cardinality
  `error.type` value;
- allowed error values should be declared once in the metrics specification;
- generated code should expose separate success and failure recording methods;
- known error attribute sets should be built once during metric initialization,
  not reconstructed for every measurement.

This follows the OpenTelemetry error convention. An empty string is a real
attribute value, not the same thing as an absent attribute. `_OTHER` is the
fallback for an unclassified failure; it must not represent success.

## Current state

- `processor/metrics/spec.yaml` declares `error.type` as a normal attribute set.
- Generated JSON duration methods therefore require `errType string` on every
  call.
- `processor/json_encoder.go` and `processor/json_decoder.go` call
  `getJSONErrorType(nil)`, receive `""`, and record `error.type=""` on success.
- `processor/json.go` already classifies JSON errors into stable strings.
- Duration bucket-set generation and float normalization are already
  implemented. Do not undo the clean `1e-06, 2.5e-06, ...` output.

## Recommended YAML model

Use `errors` for an inline list and `error_set` for a named reusable set. Avoid
a singular `error` field: it sounds like one runtime error rather than a list
of allowed error types.

```yaml
error_sets:
  - name: json_decoder
    errors:
      - name: input_too_large
        value: goccia.json.input_too_large
      - name: null_rejected
        value: goccia.json.null_rejected
      - name: trailing_value
        value: goccia.json.trailing_value
      - name: syntax_error
        value: goccia.json.syntax_error
      - name: type_error
        value: goccia.json.type_error
      - name: unknown_field
        value: goccia.json.unknown_field

  - name: json_encoder
    errors:
      - name: unsupported_type
        value: goccia.json.unsupported_type
      - name: unsupported_value
        value: goccia.json.unsupported_value
      - name: marshaler_error
        value: goccia.json.marshaler_error

groups:
  - name: json_decoder
    metrics:
      - name: goccia.json.decoder.operation.duration
        type: histogram
        data_type: float
        unit: s
        error_set: json_decoder

      - name: goccia.json.decoder.input.size
        type: histogram
        unit: By
        error_set: json_decoder
```

An inline form should also be possible:

```yaml
errors:
  - name: timeout
    value: example.timeout
  - name: cancelled
    value: example.cancelled
```

Prefer `name` plus `value` over plain strings. `name` provides a stable Go
identifier while `value` remains the emitted OpenTelemetry value.

## Proposed specification types

Use Go names that make the OpenTelemetry concept explicit, even though the
YAML remains concise:

```go
type ErrorType struct {
    Name        string `yaml:"name"`
    Value       string `yaml:"value"`
    Description string `yaml:"description"`
}

type ErrorSet struct {
    Name   string       `yaml:"name"`
    Errors []*ErrorType `yaml:"errors"`
}

type Metric struct {
    // Existing fields...
    Errors   []*ErrorType `yaml:"errors"`
    ErrorSet string       `yaml:"error_set"`
}

type Spec struct {
    // Existing fields...
    ErrorSets []*ErrorSet `yaml:"error_sets"`
}
```

Decide and document whether inline `errors` and `error_set` may be combined.
The simplest first version should make them mutually exclusive.

## Generated error types

Named error sets should generate typed constants once, not once per metric.
For example, generate an `error_types.metrics.go` file:

```go
type JsonDecoderErrorType string

const (
    JsonDecoderErrorTypeInputTooLarge JsonDecoderErrorType = "goccia.json.input_too_large"
    JsonDecoderErrorTypeNullRejected  JsonDecoderErrorType = "goccia.json.null_rejected"
    JsonDecoderErrorTypeTrailingValue JsonDecoderErrorType = "goccia.json.trailing_value"
    JsonDecoderErrorTypeSyntaxError   JsonDecoderErrorType = "goccia.json.syntax_error"
    JsonDecoderErrorTypeTypeError     JsonDecoderErrorType = "goccia.json.type_error"
    JsonDecoderErrorTypeUnknownField  JsonDecoderErrorType = "goccia.json.unknown_field"
    JsonDecoderErrorTypeOther         JsonDecoderErrorType = "_OTHER"
)
```

Generate `_OTHER` automatically for every set. Reject user entries named or
valued as the generated fallback unless the design explicitly changes to let
users declare it themselves.

For inline errors, derive the type name from the metric title. Named sets are
preferred because inline metric-derived type names may be long.

## Generated recording API

A histogram with errors should get three APIs:

```go
// Success: no error.type attribute.
func (m *JsonDecoder) RecordGocciaJsonDecoderOperationDuration(
    ctx context.Context,
    value float64,
)

// Failure: records a validated error.type.
func (m *JsonDecoder) RecordGocciaJsonDecoderOperationDurationWithErrorType(
    ctx context.Context,
    value float64,
    errorType JsonDecoderErrorType,
)

// Existing escape hatch for callers that already own a complete attribute set.
func (m *JsonDecoder) RecordGocciaJsonDecoderOperationDurationWithAttributes(
    ctx context.Context,
    value float64,
    attributes metric.MeasurementOption,
)
```

The ordinary `Record...` method must not accept an error string. This makes the
fast and semantically correct success path the default.

If a metric also has ordinary attributes, keep those arguments on both the
success and error methods, with `errorType` as the final argument.

## Why cache error attributes

The naming is easy to misread:

- OpenTelemetry's `metric.WithAttributes(...)` constructs an attribute set and
  does non-trivial work on every call.
- Goccia's `RecordWithAttributes(...)` accepts an already-built
  `metric.MeasurementOption`; it does not itself construct the attributes.

Today, `telemetry.FloatHistogram.Record(ctx, value, dynamicAttrs...)` passes two
options to OpenTelemetry: the cached stage attributes and a freshly created
dynamic option. OpenTelemetry must allocate/build the dynamic set and merge it
with the stage set for every measurement.

Instead, build one complete option per known error during `InitMetrics`:

```go
m.jsonDecoderErrorTypeAttrs = map[JsonDecoderErrorType]metric.MeasurementOption{
    JsonDecoderErrorTypeSyntaxError: tel.NewMetricAttributes(
        attribute.String(
            "error.type",
            string(JsonDecoderErrorTypeSyntaxError),
        ),
    ),
    // Other known errors...
}
```

`tel.NewMetricAttributes` includes `stage.kind` and `stage.name`, so the cached
option is complete. The recording method can pass one option and avoid a
per-record set construction and merge:

```go
func (m *JsonDecoder) RecordGocciaJsonDecoderOperationDurationWithErrorType(
    ctx context.Context,
    value float64,
    errorType JsonDecoderErrorType,
) {
    attrs, ok := m.jsonDecoderErrorTypeAttrs[errorType]
    if !ok {
        attrs = m.jsonDecoderErrorTypeAttrs[JsonDecoderErrorTypeOther]
    }

    m.gocciaJsonDecoderOperationDuration.RecordWithAttributes(ctx, value, attrs)
}
```

Cache one map per unique error set within a generated metric group and reuse it
across all histograms in that group. A measurement option is attribute-specific,
not instrument-specific.

The success path remains the fastest path:

```go
m.gocciaJsonDecoderOperationDuration.Record(ctx, value)
```

This uses the histogram wrapper's existing cached base option.

### Metrics that also have ordinary attributes

Arbitrary ordinary attribute values cannot be fully cached. For the first
implementation, choose one of these policies and test it:

1. Optimize only metrics with no ordinary attributes. For metrics with both
   ordinary attributes and errors, build the complete combined set per call.
2. Temporarily reject `errors`/`error_set` together with
   `attributes`/`attribute_set` and add combined support later.

Option 1 is more general; option 2 is a smaller initial change. The JSON
metrics need only the optimized no-ordinary-attributes path.

## Validation rules

- Error-set names must be non-empty and unique.
- Error names must be non-empty and unique within their effective set.
- Error values must be non-empty and unique within their effective set.
- `error_set` must reference an existing set.
- Inline `errors` and `error_set` should initially be mutually exclusive.
- `_OTHER` is reserved if the generator adds it automatically.
- Initially support errors only for histogram metrics. Current counters are
  observable counters and cannot attach attributes to individual increments.
- Generated Go identifiers must be unique after camel-case conversion.
- Unknown typed values passed at runtime must use the cached `_OTHER` option,
  never create a new time series from the unknown string.

## Generator changes

- [ ] Extend `pkg/spec.go` with `ErrorType`, `ErrorSet`, metric error fields,
      and top-level `error_sets`.
- [ ] Add an error-set registry and validation/resolution in
      `pkg/validator.go`.
- [ ] Add templates for shared error types and constants.
- [ ] Extend `metricsFile`/generator template data with the effective error
      types used by each group.
- [ ] Generate one cached attribute-option map per unique error set used by a
      group.
- [ ] Update the histogram template with success and `WithErrorType` methods.
- [ ] Preserve the existing `WithAttributes` method.
- [ ] Add required `attribute` and `metric` imports when errors are configured.
- [ ] Include allowed error types in generated Markdown documentation.

## JSON migration

- [ ] Replace the `error_type` attribute set in
      `processor/metrics/spec.yaml` with `json_encoder` and `json_decoder`
      error sets.
- [ ] Regenerate processor metric files.
- [ ] Remove the handwritten JSON error-string constants from
      `processor/json.go`; use the generated typed constants as the source of
      truth.
- [ ] Change `getJSONErrorType` to return the generated encoder/decoder error
      type. If one function cannot cleanly return two distinct set types, use a
      common JSON error set or split it into encoder and decoder classifiers.
- [ ] On success, call the generated ordinary `Record...` method.
- [ ] On failure, call `Record...WithErrorType`.
- [ ] Apply the same conditional behavior to decoder input size.
- [ ] Remove the temporary `TODO! fix error type to not be included on success`
      comments from the JSON workers.

## Tests and benchmarks

### Generator tests

- [ ] Load and resolve valid inline errors and named error sets.
- [ ] Reject missing names/values, duplicates, unknown sets, reserved `_OTHER`,
      and invalid metric types.
- [ ] Assert constants are emitted once when several metrics reuse a set.
- [ ] Assert success methods do not accept or record `error.type`.
- [ ] Assert error methods use the typed set and `_OTHER` fallback.
- [ ] Assert cached options include the base stage attributes.
- [ ] Keep the bucket-bound regression test passing.

### Processor/telemetry tests

- [ ] Successful JSON encode/decode measurements have no `error.type`.
- [ ] Failed measurements contain the expected classified value.
- [ ] Unknown failures use `_OTHER`.
- [ ] `stage.kind` and `stage.name` remain present on cached error measurements.

### Benchmarks

Compare at least these paths with `-benchmem` and a real SDK meter provider:

1. success using cached base attributes;
2. failure using dynamically constructed attributes;
3. failure using the generated cached combined option.

The expected order is success, cached failure, dynamic failure. Do not require
zero total allocations from the SDK; verify that the cached path removes the
extra attribute construction/merge cost.

## Completion checklist

- [ ] `go generate ./processor/metrics` is deterministic and idempotent.
- [ ] `go test ./...` passes from the repository root.
- [ ] `go test ./...` passes from `cmd/metrics-gen` (it is a separate module).
- [ ] `git diff --check` passes.
- [ ] Generated JSON success series omit `error.type` entirely.
- [ ] Generated error series use only declared values or `_OTHER`.

