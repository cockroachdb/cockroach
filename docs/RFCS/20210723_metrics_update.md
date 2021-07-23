- Feature Name: Metrics Update
- Status: draft
- Start Date: 2021-07-19
- Authors: Rima Deodhar
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [67756](https://github.com/cockroachdb/cockroach/issues/67756)

# Summary
This RFC describes the motivation and mechanism for adding additional metadata to the 
CRDB metrics framework and exposing it through an endpoint along the lines of the 
prometheus endpoint (_status/vars). We introduce the term “indicator” which is defined 
as a meaningful aggregation over one or more metrics that gives an insight into the 
health of the system. The additional metadata structure introduced in this RFC will be 
organized to infer one or more indicators from a given metric.

Currently, our metrics insufficiently describe what they measure, they lack definition 
on the possible range of values, about what it means to observe variance, about how to 
aggregate metrics across multiple nodes/clusters. The immediate goal of adding and 
exposing this additional metadata (a.k.a. indicator information) is to bridge this 
gap and provide better documentation on the metrics we expose and how to use them. 
In addition, we also want to create an engineering framework that will educate and 
enforce engineers defining a new metric to specify not only what the metric is but 
how to use it. This will empower our engineers, SREs, TSEs and even our end users to 
disambiguate metrics and leverage indicators for debugging and monitoring the health 
of the system.

# Motivation
In cockroachDB, metrics have been defined as a source of data values along with 
metadata that describe them. The metadata is very basic (name, help message, 
type of metric) and contains limited information on the type of data the metric 
exposes and how to use it. 
As a result, our metrics insufficiently describe what they measure, they lack 
definition on the possible range of values, about what it means to observe variance, 
about how to aggregate metrics across multiple nodes (for example, using average 
vs sum), and how to aggregate metrics across multiple clusters.

This has led to some undesirable side effects such as an over reliance on debug.zip 
instead of direct access to timeseries data for debugging. By introducing the concept
of “indicators'' and providing an engineering framework to specify and expose this 
information for metrics, cockroachDB will gain improvement in the following areas:
- **Observability**:
  With a growing operational load, we need to improve observability by making the system 
  understandable to non-experts such as SRE, TSE, and potentially even the customer. 
  Indicators achieve this end. By leveraging this additional indicator information, 
  we can empower our end users, SREs and TSEs to build meaningful aggregations over our 
  metrics using powerful existing tools like Prometheus and Grafana. 
  This will enable informed debugging and robust conclusions drawn by non-experts which
  will directly lead to less operational load on DB engineers, in addition to higher reliability. 
  The indicator information can also be used to drive improved alerting which is 
  particularly important for the cloud product. Without excellent alerting, reliability 
  will simply not be high enough, regardless of how robust CRDB is. Indicators can help us 
  achieve better alerting and reliability as we continue to expand and support our cloud 
  product offerings.
  
# Technical Design:

## Data:

### Metric Metadata Updates:
We will add fields to metrics metadata to specify additional information on how to 
construct an indicator from a metric. The proposed design to update the metric metadata
is as follows:

```protobuf
// ValueRange indicates the range of values for a metric.
// The lowerBound and upperBound values will be treated as inclusive
// bounds. That is the metric value will be expected to lie within
// [lowerBound, upperBound]
message ValueRange {
  optional string lowerBound = 1 [(gogoproto.nullable) = false];
  optional string upperBound = 2 [(gogoproto.nullable) = false];
}

// Metadata holds metadata about a metric. It must be
// embedded in each metric object. It's used to export
// information about the metric to Prometheus and for Admin
// UI charts.
message Metadata {
// <existing_fields>
  required string name = 1 [(gogoproto.nullable) = false];
  required string help = 2 [(gogoproto.nullable) = false];
  required string measurement = 3 [(gogoproto.nullable) = false];
  required Unit unit = 4 [(gogoproto.nullable) = false];
  optional io.prometheus.client.MetricType metricType = 5 [(gogoproto.nullable) = false];
  repeated LabelPair labels = 6;
// </existing_fields>
  
// <new_fields>
  // implementationDetails will provide a technical explanation on how
  // a metric has been implemented. This is intended to be used by
  // engineers on understanding and maintaining a metric.
  required string implementationDetails = 7 [(gogoproto.nullable) = false];
  // limits will define the *possible* value range for the
  // metric.
  optional ValueRange limits = 8 [(gogoproto.nullable) = false];
  // expected will define the *reasonable* value range for
  // the metric.
  // expected and limits ValueRanges can be used in
  // conjunction to get an idea of the spectrum of
  // acceptable and normal values for that metric.
  optional ValueRange expected = 9 [(gogoproto.nullable) = false];
// </new_fields>
}
```
We will add the following fields to the `Metadata` protobuf to provide additional 
information about the metric to help derive an indicator from it:
* `implementationDetails`: 
  This will cover technical details on how the metric has been implemented 
  in CRDB.The audience for this field are other developers in addition to operators. This will 
  serve as the source of truth on how a metric has been implemented and should be kept up to 
  date with any updates to the metric. This information is important for documentation and 
  future maintenance of the metric.
* `limits`: 
  This will outline the possible upper and lower bound values for a metric.
* `expected`: 
  This will outline the expected upper and lower bound values for a metric.

The `limits` and `expected` fields can be used in conjunction to infer whether a 
particular metric value stands within the normal expected bounds, outside of the 
expected bounds but still within the limits and completely outside the limits. 
This will give an important indication on how the metric value should be treated 
and can form a good basis for formulating alerts.
The limits and expected fields are of type ValueRange which will encapsulate the 
upper and lower bound values for the metric. The unit for the values contained 
within a ValueRange will be the same as the unit for a metric which is already defined 
within the Metadata message.

### Indicator
The indicator protobuf will contain information about an indicator that can be inferred
from one or more metrics. As mentioned earlier, an indicator is defined as a meaningful
aggregation over one or more metrics that gives an insight into the health of the 
system. This data will be exposed through an API for improved documentation and 
understanding of our metrics. The primary consumers of indicators are intended to be 
non DB engineers such as SREs, TSEs, DB operators and our customers.

```protobuf
message Indicator {
  // name is descriptive name for the indicator
  // (for e.g. query_error_rate and will be used to uniquely identify
  //  the indicator)
  required string name = 1 [(gogoproto.nullable) = false];
  // help is a string message with more descriptive details for the
  // indicator
  required string help = 2 [(gogoproto.nullable) = false];
  // aggregationMethod is a string expression indicating how the
  // metrics constructing the indicator need to be aggregated.
  // The aggregation method should be defined using the Prometheus
  // aggregation operator syntax.
  required string aggregation = 3 [(gogoproto.nullable) = false];
  // unit is the unit of measurement for the indicator
  // (for e.g. unit for error rate would be count/sec)
  required Unit unit = 4 [(gogoproto.nullable) = false];
  // limits will define the *possible* value range for the indicator.
  required ValueRange limits = 5;
  // expected will define the *reasonable* value range for the metric.
  // expected and limits ValueRanges can be used in conjunction to get
  // an idea of the spectrum of acceptable and normal values for that
  // metric.
  required ValueRange expected = 6;
  // metrics is the list of one or more metric metadata involved in the
  // computation of the Indicator.
  repeated Metadata metrics = 7;
}
```

The indicator protobuf will contain all the fields necessary to construct an 
indicator from a set of one or more metrics. The name and help fields should 
be descriptive enough to explain how an indicator is to be used to gain 
insight into the health of the system. Since the consumers of this information 
are intended to be non DB engineers, it is important that these fields should 
contain detailed and verbose explanations on how this indicator is to be used 
for monitoring and debugging the health of the system. The aggregation field 
combined with the metrics field will describe how the indicator is constructed. 
The aggregation should be specified using the [Prometheus aggregation operator 
syntax](https://prometheus.io/docs/prometheus/latest/querying/operators/). As with 
metrics metadata, the limits and expected fields will indicate what the possible 
and expected values for an indicator are. These can be used as the thresholds while 
defining an alert for the metric.

The reason for choosing the Prometheus aggregation syntax over keeping it 
free-form, was to introduce some element of structure to the aggregation rules.
Having some structure allows us to:
1. Set up lint rules and unit test frameworks to verify the correctness of 
   aggregation rules being defined for indicators.
2. Potentially, automate dashboard generation for indicators in the future by 
   using a structured and widely used prometheus operator format. 
3. Exposing indicator aggregation format in a manner that the majority of 
   consumers of the information will be familiar with will make it easier for 
   our audience to understand and use indicators.

**Examples:**
With this structure defined, this section explores some examples on how 
existing metrics can be converted to indicators. Keep in mind that one 
indicator may involve more than one metric and one metric can be associated 
with more than one indicator. I used two of our [production alerts](https://github.com/cockroachdb/cockroach/blob/4d987b3f0788c98fa358715b336a1c55e254ee50/monitoring/rules/alerts.rules.yml) 
as inspiration for the examples in this section.

**Example 1:**

**Metric:** sys.uptime

**Indicator**: Flapping instances in a cluster i.e. instance restarts
```go
sys_uptime := Metadata {
  Name:                  "sys.uptime",
  Help:                  "Process uptime",
  Measurement:           "Uptime",
  Unit:                  Unit_SECONDS,
  MetricType:            prometheusgo.MetricType_GAUGE,
  ImplementationDetails: `This metric is computed as the difference between the 
  time of recording the metric and the time the node/sql instance was started. 
  It is computed within the runtime system of CRDB`,
  Limits:                ValueRange{
    LowerBound: "0",
    UpperBound: "+Inf",
  },
  Expected:              ValueRange{
    LowerBound: "0",
    UpperBound: "+Inf",
  },
}
i := Indicator{
  Name:        "instance_flap_cluster",
  Help:        "Number of instance restarts within a cluster in the past five minutes",
  Aggregation: "sum by (cluster)(resets(sys_uptime{job=\"cockroachdb\"}[5m]))",
  Unit:        Unit_COUNT,
  Limits:      ValueRange{
    LowerBound: "0",
    UpperBound: "+Inf",
  },
  Expected:    ValueRange{
    LowerBound: "0",
    UpperBound: "2",
  },
  Metrics:     { sys_uptime },
}
```

**Example 2:**

**Metrics:** sys_fd_open, sys_fd_softlimit

**Indicator:** Open FD count ratio
```go
sys_fd_open := Metadata{
  Name:                  "sys.fd.open",
  Help:                  "Process open file descriptors",
  Measurement:           "File Descriptors",
  Unit:                  Unit_COUNT,
  MetricType:            prometheusgo.MetricType_GAUGE,
  ImplementationDetails: `This is implemented within the runtime system of CRDB. 
  We periodically query this system to determine the current number of open 
  file descriptors. We use the gosigar package to determine the open file
  descriptor count.`,
  Limits:                ValueRange{
    LowerBound: "0",
    // upperBound can vary based on
    // platform(Linux, Windows etc) but
    // we can provide a tentative upper
    // bound
    UpperBound: "1000000",
  },
  Expected:              ValueRange{
    LowerBound: "0",
    UpperBound: "700000",
  },
}

sys_fd_softlimit := Metadata{
  Name:                  "sys.fd.softlimit",
  Help:                  "Process open FD soft limit",
  Measurement:           "File Descriptors",
  Unit:                  Unit_COUNT,
  MetricType:            prometheusgo.MetricType_GAUGE,
  ImplementationDetails: `This is implemented within the runtime system of 
CRDB. We periodically query this system to determine the current softlimit 
on open file descriptors. We use the gosigar package to determine the soft 
limit on the platform.`,
  Limits:                ValueRange{
    LowerBound: "0",
    // upperBound can vary based on
    // platform(Linux, Windows etc) but
    // we can provide a tentative upper
    // bound
    UpperBound: "1000000",
  },
  Expected:              ValueRange{
    LowerBound: "1000000",
    UpperBound: "1000000",
  },
}

i := Indicator{
  Name:        "open_fd_count_ratio",
  Help:        `Ratio of open file descriptors to the open file descriptor 
soft limit for the platform.`,
  Aggregation: "sys_fd_open{job=\"cockroachdb\"} / sys_fd_softlimit{job=\"cockroachdb\"}\n",
  Unit:        Unit_RATIO,
  Limits:      ValueRange{
    LowerBound: "0",
    UpperBound: "1",
  },
  Expected:    ValueRange{
    LowerBound: "0",
    UpperBound: "0.8",
  },
  Metrics:     { sys_fd_count, sys_fd_softlimit },
}
```


### Indicator Registry
To create and register an indicator, we will create a new IndicatorRegistry 
within the metrics package. IndicatorRegistry will contain information about 
all the currently registered indicators. This will be used by the introspection
API to expose indicators through an HTTP endpoint.

```go
// IndicatorsData interface should be implemented by all metrics
// systems to expose indicators.
type IndicatorsData interface {
    GetIndicators() []Indicator
}

// An IndicatorRegistry stores a list of indicators. It provides
// a simple way of iterating over them and exporting them in
// a JSON format.
type IndicatorRegistry struct {
  syncutil.Mutex
  indicators []Indicator
}

// NewIndicatorRegistry creates a new Registry.
func NewIndicatorRegistry() *IndicatorRegistry

// AddIndicators adds multiple indicators to the
// IndicatorRegistry.
func (ri *IndicatorRegistry) AddIndicators(IndicatorsData)

// AddIndicator adds the passed-in indicator to the
// IndicatorRegistry.
func (ri *IndicatorRegistry) AddIndicator(Indicator)

// MarshalJSON marshals all registered indicators to JSON.
func (ri *IndicatorRegistry) MarshalJSON() ([]byte, error)
```
We will create and add an indicator registry to the [server](https://github.com/cockroachdb/cockroach/blob/0119b2b573c6065e3d5a785c80e5b98bd984c168/pkg/server/server.go) 
and [CCL sql proxy server](https://github.com/cockroachdb/cockroach/blob/3819f5e6d46d507aa8fcb4572e0ef075f488bb49/pkg/ccl/sqlproxyccl/server.go#L40) 
structures along the lines of [metrics.Registry](https://github.com/cockroachdb/cockroach/blob/4d987b3f0788c98fa358715b336a1c55e254ee50/pkg/util/metric/registry.go#L31).
The indicator registry will be used to register and export indicator information.

So the indicators covered in the above examples, will be created and 
registered with the indicator registry as shown below:

The indicators `instance_flap_cluster` and `open_fd_count_ratio` will be 
created within the [RuntimeStatSampler](https://github.com/cockroachdb/cockroach/blob/3021eec762bd1ae929becbf9694b189eb25edf86/pkg/server/status/runtime.go#L244) 
by implementing the IndicatorsData interface:

`runtime.go`:
```go
// Indicators
var (
  instanceFlapIndicator = metric.Indicator {
    Name:        "instance_flap_cluster",
    Help:        `Number of instance restarts within a cluster in the past five minutes. 
    This indicators assumes that rolling restarts or rolling upgrades leave at least 3 
    minutes between each node being updated or restarted`,
    Aggregation: "sum by (cluster)(resets(sys_uptime{job=\"cockroachdb\"}[5m]))",
    Unit:        metric.Unit_COUNT,
    Limits:      metric.ValueRange{
        LowerBound: "0",
        UpperBound: "+Inf",
    },
    Expected:    metric.ValueRange{
        LowerBound: "0",
        UpperBound: "2",
    },
    Metrics:     []*metric.Metadata{&metaUptime},
  }
  
  openFDCountIndicator = metric.Indicator{
    Name:        "open_fd_count_ratio",
    Help:        "Ratio of open file descriptors to the open file descriptor soft limit for the platform",
    Aggregation: "sys_fd_open{job=\"cockroachdb\"} / sys_fd_softlimit{job=\"cockroachdb\"}",
    Unit:        metric.Unit_RATIO,
    Limits:      metric.ValueRange{
      LowerBound: "0",
      UpperBound: "1",
    },
    Expected:    metric.ValueRange{
      LowerBound: "0",
      UpperBound: "0.8",
    },
    Metrics:     []*metric.Metadata{&metaFDOpen, &metaFDSoftLimit},
  }
)

// GetIndicators will return a list of indicators relevant to the
// runtime system.
func (rsr *RuntimeStatSampler) GetIndicators() []metric.Indicator {
  var indicators []metric.Indicator
  indicators = append(indicators, instanceFlapIndicator)
  indicators = append(indicators, openFDCountIndicator)
  return indicators
}
```

Register the indicators with the indicator registry:
```go
runtimeSampler := status.NewRuntimeStatSampler(ctx, clock)
indicatorRegistry.AddIndicators(runtimeSampler)
```

### Introspection API
To expose this information, we will add an introspection API that will publish
all declared indicators for the metrics via an HTTP endpoint along the lines 
of the _status/vars endpoint. The metrics metadata along with the indicator 
data will be fetched and displayed in JSON format.  The data exposed through 
this endpoint can be used to expose this additional documentation on our 
metrics and how to use them as indicators. In this section, we will go over 
the details on the API and the response format.

**Endpoint:**
We will expose a new unauthenticated http endpoint called 
_status/healthindicators. This will export the available indicators as JSON.

**HTTP Handler for Server and CCL proxy server:**
We will add a new HTTP handler method called handleIndicators which will 
expose the indicator information in a JSON format using the MarshalJSON 
method from the IndicatorRegistry.

A rough skeleton of this method would be as follows:
```go
func (s *Server) handleIndicators(w http.ResponseWriter, r *http.Request) {
w.Header().Set(httputil.ContentTypeHeader,     
httputil.PlaintextContentType)
indicators, err := s.indicatorRegistry.MarshalJson()
if err != nil {
log.Errorf(r.Context(), "%v", err)
http.Error(w, err.Error(), http.StatusInternalServerError)
return
}
w.Write(indicators)
}
```

**Output Data Format:**
All registered indicators will be exported in a JSON format through the HTTP 
endpoint. The output will contain all relevant details necessary for consumers
of this data (SRE, TSE, end users) to graph the metric through powerful tools 
like Prometheus and Grafana.

Example view of how the data exposed through the API will look:
```json
{
  "indicators": [
    {
      "name": "instance_flap_cluster",
      "help": "Number of instance restarts within a cluster in the past five minutes",
      "aggregation": "sum by (cluster)(resets(sys_uptime{job=\"cockroachdb\"}[5m]))",
      "unit": "COUNT",
      "limits": {
        "lowerBound": "0",
        "upperBound": "+Inf"
      },
      "expected":{
        "lowerBound": "0",
        "upperBound": "2"
      },
      "metrics": [
        {
          "name": "sys.uptime",
          "help": "Process uptime",
          "unit": "SECONDS",
          "implementationDetails": "This metric is computed as the difference between the time of recording the metric and the time the node/sql instance was started. It is computed within the runtime system of CRDB",
          "limits": {
            "lowerBound": "0",
            "upperBound": "+Inf"
          },
          "expected": {
            "lowerBound": "0",
            "upperBound": "+Inf"
          }
        }
      ]
    },
    {
      "name": "open_fd_count_ratio",
      "help": "Ratio of open file descriptors to the open file descriptor soft limit for the platform.",
      "aggregation": "sys_fd_open{job=\”cockroachdb\”} / sys_fd_softlimit{job=\”cockroachdb\”}",
      "unit": "RATIO",
      "limits": {
        "lowerBound": "0",
        "upperBound": "1"
      },
      "expected": {
        "lowerBound": "0",
        "upperBound": "0.8"
      },
      "metrics": [
        {
          "name": "sys.fd.open",
          "help": "Process open file descriptors",
          "unit": "COUNT",
          "implementationDetails": "This is implemented within the runtime system of CRDB. We periodically query this system to determine the current number of open file descriptors. We use the gosigar package to determine the open file descriptor count.",
          "limits": {
            "lowerBound": "0",
            "upperBound": "1000000"
          },
          "expected": {
            "lowerBound": "0",
            "upperBound": "700000"
          }
        },
        {
          "name": "sys.fd.softlimit",
          "help": "Process open FD soft limit",
          "measurement": "File Descriptors",
          "unit": "COUNT",
          "implementationDetails": "This is implemented within the runtime system of CRDB. We periodically query this system to determine the current softlimit on open file descriptors. We use the gosigar package to determine the soft limit on the platform.",
          "limits": {
              "lowerBound": "0",
              "upperBound": "1000000"
          },
          "expected": {
            "lowerBound":"1000000",
            "upperBound":"1000000"
          }
        }
      ]
    }
  ]
}
```
## Drawbacks
By introducing the term “indicators”, we are creating a new terminology for 
aggregated metrics which is not widely understood in the industry. This 
creates a learning curve to educate our primary audience on what indicators 
mean and how they should be used. This is not ideal when the primary goal is 
to make CRDB metrics easy to use and understand by our support team and end 
users. This can be mitigated by only using indicators as an internal reference
to aggregated metrics. We need not expose this term to the end users of this data.

## Rationale and Alternatives:
There were mainly two design alternatives that were considered:
1. **Add aggregation method and other helper fields within the metrics 
   metadata directly without creating a separate indicator protobuf:** 
   This would keep the design simpler as all changes would be restricted to 
   the metrics metadata. However, this design is not very flexible as it does 
   not provide for a way to indicate aggregation of two or more metrics. 
   There are a fair number of indicators which would require aggregation 
   across multiple metrics (for example, error rate).
2. **Keep the aggregation field within indicators as a free-form text message
   instead of using something structured like the Prometheus aggregation 
   syntax:** However, this was rejected in favor of keeping it structured from 
   the get-go. By enforcing some structured way of providing aggregation 
   information, it will allow us to build linters and unit test frameworks 
   which will enforce the validity of the aggregation field.

## Future Work:
While using the indicator API output for programmatic consumption is out of 
scope for this RFC, we would like to build upon this work to provide a 
mechanism to automate dashboard generation or alertings in the future. 
The north star for this would ideally be a way to automate generation of 
Grafana-ready prod dashboards and basic alerts.
