- Feature Name: Metrics Update
- Status: in-progress
- Start Date: 2021-07-19
- Authors: Rima Deodhar
- RFC PR: [68082](https://github.com/cockroachdb/cockroach/pull/68082)
- Cockroach Issue: [67756](https://github.com/cockroachdb/cockroach/issues/67756)

# Summary
This RFC describes the motivation and mechanism for upgrading the metrics framework
with additional metadata and a framework for automatically generating prometheus
style [alert](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)
and [recording](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/)
rules from our metrics. This will involve implementing a library that engineers can use
to generate alert and recording rule templates. These generated rules will be exposed in
YAML format through an HTTP endpoint.

Currently, our metrics insufficiently describe what they measure, they lack definition
on the possible range of values, about what it means to observe variance, about how to 
aggregate metrics across multiple nodes/clusters. Engineers adding a metric can leverage
this new framework to specify alerts and aggregation(recording) rule templates for metrics. 
This will enable engineers not just to create and define a metric but also specify how
the metric should be aggregated and alerted on. This will empower our engineers, SREs,
TSEs and even our end users to disambiguate metrics and leverage these rules for
debugging and monitoring the health of the system.

*Disclaimer: Recording rules is the prometheus term for aggregation rules.
For the rest of this RFC,the terms "recording rules" and "aggregation rules" will be
used synonymously.*

# Motivation
In CockroachDB, metrics have been defined as a source of data values along with 
metadata that describe them. The metadata is very basic (name, help message,
type of metric) and contains limited information on the type of data the metric 
exposes and how to use it.
As a result, our metrics insufficiently describe what they measure, they lack
definition on the possible range of values, about what it means to observe variance,
about how to aggregate metrics across multiple nodes (for example, using average
vs sum), and how to aggregate metrics across multiple clusters.

By improving the metrics metadata and building an automated alert and recording rule gen
framework, CockroachDB will gain improvement in the following areas:
- **Observability**:
  With a growing operational load, we need to improve observability by making the system
  understandable to non-experts such as SRE, TSE, and potentially even the customer.
  The metadata improvements and framework described in this RFC will be a step in
  this direction. By enabling engineers to specify more details about how a metric
  should be used for alerting and monitoring the health of the system, we can empower
  our end users, SREs and TSEs to build meaningful aggregations over our metrics using
  powerful existing tools like Prometheus and Grafana.
  This will enable informed debugging and robust conclusions drawn by non-experts which
  will directly lead to less operational load on DB engineers, in addition to higher
  reliability. Improved alerting is also particularly important for the cloud product.
  Without excellent alerting, reliability will simply not be high enough, regardless of
  how robust CRDB is. A more powerful metrics framework can help us achieve better
  alerting and reliability as we continue to expand and support our cloud product offerings.
- **Documentation**
  Supplementing the the `Metadata` protobuf with implementation details about the metric
  will make the metrics easier to understand and maintain.
   
  
# Technical Design

## Data

### Metric Metadata Updates
We will add a new field to metrics metadata to specify additional information on how
the metric has been implemented. The proposed design to update the metric metadata
is as follows:

```protobuf
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
// </new_fields>
}
```
We will add the following field to the `Metadata` protobuf to provide additional
information about the metric:
* `implementationDetails`: 
  This will cover technical details on how the metric has been implemented
  in CRDB.The audience for this field are other developers in addition to operators.
  This should be kept up to date with any updates to the metric. This information is
  important for documentation and future maintenance of the metric. It will also be
  useful from a debugging perspective as engineers/oncalls attempting to understand
  an anomalous value from the metric have a way to quickly determine how the metric
  has been implemented without having to comb through complex code/contact the
  engineer who implemented the metric. This will reduce operational load on
  oncalls and support staff and indirectly reduce MTTR which would be a big win.

For example, metrics with implementation details included will look as below:
```go
metaUptime := Metadata {
  Name:                  "sys.uptime",
  Help:                  "Process uptime",
  Measurement:           "Uptime",
  Unit:                  Unit_SECONDS,
  MetricType:            prometheusgo.MetricType_GAUGE,
  ImplementationDetails: `This metric is computed as the difference between the
  time of recording the metric and the time the node/sql instance was started.
  It is computed within the runtime system of CRDB`,
}

metaFDOpen := Metadata{
	Name:                  "sys.fd.open",
	Help:                  "Process open file descriptors",
	Measurement:           "File Descriptors",
	Unit:                  Unit_COUNT,
	MetricType:            prometheusgo.MetricType_GAUGE,
	ImplementationDetails: `This is implemented within the runtime system of CRDB.
  We periodically query this system to determine the current number of open
  file descriptors. We use the gosigar package to determine the open file
  descriptor count.`,
}

metaFDSoftLimit := Metadata{
	Name:                  "sys.fd.softlimit",
	Help:                  "Process open FD soft limit",
	Measurement:           "File Descriptors",
	Unit:                  Unit_COUNT,
	MetricType:            prometheusgo.MetricType_GAUGE,
	ImplementationDetails: `This is implemented within the runtime system of
CRDB. We periodically query this system to determine the current softlimit
on open file descriptors. We use the gosigar package to determine the soft
limit on the platform.`,
}

```
## Alert and Aggregation Rule Generation Framework
This section will cover:
1. The interfaces and struct design for specifying alert and aggregation rules.
2. The API to expose these alert and aggregation rules in a YAML format.
3. CI tests to enforce the validity of these rules.

### Interface

#### Rules

The alert and aggregation rule structure for CRDB metrics follows the Prometheus design
for specifying alert and recording rules very closely. Aligning with the Prometheus design
will make the generated alert and aggregation rules easily usable with Prometheus which is
widely used in the industry and within Cockroach Labs for monitoring our metrics.
It will be easy for our end users to leverage these rules as they will be in a well
documented and understood format and will reduce the learning curve. Also, given that we
already expose our metrics to a prometheus endpoint (`_status/vars`), it logically follows
for CRDB to expose alerts and aggregation rules compatible with Prometheus.

```go
// Rule interface exposes an API for alerting and aggregation rules to be
// consumed.
type Rule interface {
	Name() string
	Labels() []LabelPair
	Expr() string
	Help() string
	// See endpoint API section for details on PrometheusRuleNode.
	ToPrometheusRuleNode() (ruleGroupName string, ruleNode PrometheusRuleNode)
}

type AlertingRule struct {
	// name of the rule.
	name string
	// expr should be in the Prometheus expression syntax.
	expr        string
	annotations []LabelPair
	// Labels to add or overwrite for each alert.
	labels []LabelPair
	// This will be the recommended hold duration
	// for the alert. This should be treated as optional
	// and left unset if there is no recommendation. This
	// maps to the 'for' field within a prometheus alert.
	recommendedHoldDuration time.Duration
	// help provides a descriptive message around
	// the rule
	help string
}

type AggregationRule struct {
	// name of the rule.
	name string
	// expr should be in the Prometheus expression syntax.
	expr string
	// Labels to add or overwrite for each rule.
	labels []LabelPair
	// help provides a descriptive message around
	// the rule
	help string
}

// Alerting and AggregationRule should implement the Rule interface.
var _ Rule = &AlertingRule{}
var _ Rule = &AggregationRule{}

func NewAlertingRule(
	name string,
	expr string,
	annotations []LabelPair,
	labels []LabelPair,
	recommendedHoldDuration time.Duration) (AlertingRule, error) {
    if _, err := promql.ParseExpr(expr); err != nil {
        return AlertingRule{}, err
    }
    return AlertingRule{
    	name:                    name,
    	expr:                    expr,
    	annotations:             annotations,
    	labels:                  labels,
    	recommendedHoldDuration: recommendedHoldDuration,
  }
}
```

`AlertingRule` and `AggregationRule` will be used to construct alerts and aggregations
respectively. Each rule will contain an `expr` string which will capture the metric(s)
involved in the rule and how they should be aggregated. The expr should be in the
[Prometheus aggregation operator syntax](https://prometheus.io/docs/prometheus/latest/querying/operators/).
The `expr` should be a complete, valid prometheus expression. The reason for enforcing
valid, complete prometheus expressions is to allow for easy validation of these
expressions by leveraging the [prometheus parsing library](https://github.com/prometheus/prometheus/blob/60918b8415d928363ea4bc766d450e707035abe0/promql/parser/parse.go).
The constructors for creating alerting and aggregation rule will leverage the
Prometheus parsing library to enforce the validity of these expressions as seen
in the above pseudocode.
Allowing for these expressions to be templated will make it harder to enforce
validity of the expressions and undermine the usability of the rules. The
expressions can be used as guidelines for end users while constructing the actual
alert and recording rule yaml files for monitoring a cluster.

`AlertingRule` contains a field called `recommendedHoldDuration`. This can be used to
optionally specify a recommended hold duration for the alert while building the rule.
This could be used as a suggestion by end users for setting the hold duration while
specifying the alert.

Some examples of alerting and aggregation rules definitions:
```go
// AlertingRule involving a single metric.
instanceFlapAlert := AlertingRule{
  name:        "InstanceFlapping",
  expr:        "resets(sys_uptime[10m]) > 5",
  annotations: metric.LabelPair[
    {
      Name: "description",
      Value: `{{ $labels.instance }} for cluster {{ $labels.cluster }} restarted
    {{ $value }} time(s) in 10m`,
    }
    {
      Name: "summary",
      Value: "Instance {{ $labels.instance }} restarted",
    }
  ]
  help:      `If the number of job resets in a cluster exceeds 5 in 10 minutes,
    then it could indicate an unhealthy cluster and the job resets should be
    investigated further.`,
}

// Alerting and aggregation rules with multiple metrics.
openFDCountRatio := AggregationRule{
	name: "OpenFDCountRatio",
	expr: "sys_fd_open / sys_fd_softlimit",
	help: `Ratio of open file descriptors to the open file descriptor
   soft limit for the platform.`,
}

highOpenFDCount := AlertingRule{
	name:                    "HighOpenFDCount",
	expr:                    "sys_fd_open / sys_fd_softlimit > 0.8",
	annotations:              metric.LabelPair[
	{
		Name:  "summary",
		Value: `Too many open file descriptors of
                {{ $labels.instance }}: {{ $value}} fraction used`,
	}],
	recommendedHoldDuration: 10 * time.Minute,
	labels:                  metric.LabelPair[
	{
		Name:  "severity",
		Value: "warning",
	}],
	help: `If the ratio of open file descriptors to the file descriptor
    soft limit exceeds 0.8, it could be indicative of a high server load.
    Treat this alert as a warning.`,
}
```

**Constructing Metric Rules**

To track rules involving one or more metrics, we will create a `metrics.RuleRegistry`
along the lines of the `metrics.Registry`.
```go
type RuleRegistry struct {
	syncutil.Mutex
	rules []Rule
}

// AddRules adds rules to the registry
func (r *RuleRegistry) AddRules(rules []Rule) {
	r.Lock()
	defer r.Unlock()
	r.rules = append(r.rules, rules...)
}

// AddRule adds a single rule to the registry
func (r *RuleRegistry) AddRule(rule Rule) {
	r.Lock()
	defer r.Unlock()
	r.rules = append(r.rules, rule)
}

// GetRuleForTest will be implemented in helpers_test.go.
// See CI test section for more details.
func (r *RuleRegistry) GetRuleForTest(ruleName string) Rule {
	r.Lock()
	defer r.Unlock()
	for _, rule := range r.rules {
		if rule.Name() == ruleName {
			return rule
		}
	}
	return nil
}
```

The `RuleRegistry` will be added as a member of the `server.Server` and the
`sqlproxyccl.Server` struct. It will be initialized during Server creation similar to the
metrics registry and can be used to add rules.


For example, lets look at how the rules defined in the above examples can be tracked
using the RulesRegistry outlined above.

```go
var rules []Rule
rules = append(rules, instanceFlapAlert)
rules = append(rules, highOpenFDCount)
rules = append(rules, openFDCountRatio)
// Add rules to the rule registry within the server.
server.ruleRegistry.AddRules(rules)
```

### Endpoint exposing generated rules
To expose this information, we will add an introspection API that will publish
all declared rules for the metrics via an HTTP endpoint.The rules exposed through this
endpoint can be used as guidelines by end users such as SREs, TSEs, DB operators
and customers for monitoring and alerting. In this section, we will go over the
details on the API and the response format.

**Endpoint**

We will expose a new unauthenticated http endpoint called `api/v2/metrics/rules`.
This will export the declared rules in a YAML format.

**Rule Exporter**

The `PrometheusRuleExporter` struct will encapsulate all logic required to scrape
all declared rules and export them in the YAML format. The rough implementation
will be as below:
```go
const (
	alertRuleGroupName     = "rules/alerts"
	recordingRuleGroupName = "rules/recording"
)

// PrometheusRuleNode represents an individual rule node within the YAML output.
type PrometheusRuleNode struct {
	Record      string            `yaml:"record,omitempty"`
	Alert       string            `yaml:"alert,omitempty"`
	Expr        string            `yaml:"expr"`
	For         model.Duration    `yaml:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

// PrometheusRuleGroup is a list of recording and alerting rules.
type PrometheusRuleGroup struct {
	Name  string               `yaml:"name"`
	Rules []PrometheusRuleNode `yaml:"rules"`
}

// PrometheusRuleExporter initializes recording and alert rules once from the registry.
// These initialized values will be reused for every alert/rule scrapes.
type PrometheusRuleExporter struct {
	mu struct {
		syncutil.Mutex
		ruleGroups map[string]PrometheusRuleGroup
	}
}

func (re *PrometheusRuleExporter) ScrapeRegistry(ctx context.Context, rr *RuleRegistry) {
	// Scrape registry for all rules to construct PrometheusRuleNodes
	re.mu.Lock()
	defer re.mu.Unlock()
	for _, rule := range rr.rules {
		ruleGroupName, ruleNode := rule.ToPrometheusRuleNode()
		if ruleGroupName != alertRuleGroupName && ruleGroupName != recordingRuleGroupName {
			log.Warning(ctx, "invalid prometheus group name, skipping rule")
			continue
		}
		if _, ok := re.mu.ruleGroups[ruleGroupName]; !ok {
			re.mu.ruleGroups[ruleGroupName] = PrometheusRuleGroup{
				Name: ruleGroupName,
			}
		}
		promRuleGroup := re.mu.ruleGroups[ruleGroupName]
		promRuleGroup.Rules = append(promRuleGroup.Rules, ruleNode)
	}
}

func (re *PrometheusRuleExporter) PrintAsYAMLText() (string, error) {
	re.mu.Lock()
	defer re.mu.Unlock()
	var output []byte
	for groupName, ruleGroup := range re.mu.ruleGroups {
		bytes, err := yaml.Marshal(ruleGroup)
		if err != nil {
			return "", err
		}
		output = append(output, bytes...)
	}
	return string(output)
}

```
The `Rule` interface exports a `ToPrometheusNode()` method which will be implemented
by `AlertingRule` and `AggregationRule` to build the `PrometheusRuleNode` required
to represent the rule.

The aggregation and alerting rules will be grouped separated under
"rules/aggregation.rules" and "rules/alerts.rules" respectively in the final
YAML output.

**HTTP Handler for Server and CCL proxy server**

We will add a new HTTP handler method called handleRules which will use the
above PrometheusRuleExporter interface to export the declared rules in YAML format.
A rough skeleton of this method would be as follows:
```go
func (s *Server) handleRules(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(httputil.ContentTypeHeader, httputil.PlaintextContentType)
	s.promRulesExporter.ScrapeRegistry(r.Context(), s.metricsRegistry, s.rulesRegistry)
  rules, err := s.promRulesExporter.PrintAsYAMLText()
  if err != nil {
  	log.Errorf(r.Context(), "%v", err)
  	http.Error(w, err.Error(), http.StatusInternalServerError)
  }
  w.Write(rules)
}
```

**Output Data Format**

All registered rules will be exported in a YAML format through the HTTP
endpoint. The output will contain all relevant details necessary for consumers
of this data (SRE, TSE, end users) to build alerting and recording rules for
monitoring.

Example view of how the data exposed through the API will look:
```yaml
groups:
  - name: rules/recording.rules
      rules:
        # Ratio of open file descriptors to the open file descriptor soft limit for the platform.
        - expr: sys_fd_open / sys_fd_softlimit,
          record: "OpenFDCountRatio",
  - name:  rules/alerts.rules
      rules:
      - alert: HighOpenFDCount
        # If the ratio of open file descriptors to the file descriptor
        # soft limit exceeds 0.8, it could be indicative of a high server load.
        # Treat this alert as a warning.
        annotations:
          summary: 'Too many open file descriptors on {{ $labels.instance }}: {{ $value
          }} fraction used'
        expr: sys_fd_open / sys_fd_softlimit > 0.8
        for: 10m
        labels:
          severity: testing
      - alert: InstanceFlapping
        # If the number of job resets in a cluster exceeds 5 in 10 minutes,
        # then it could indicate an unhealthy cluster and the job resets should be
        # investigated further.
        annotations:
          description: '{{ $labels.instance }} for cluster {{ $labels.cluster }} restarted
          {{ $value }} time(s) in 10m'
          summary: Instance {{ $labels.instance }} flapping
        expr: resets(sys_uptime[10m]) > 5
```

### CI Tests
For allowing engineers to test their newly added rules, the unit testing framework
will be expanded. We will also add a new integration test for the `api/v2/metrics/rules`
endpoint which will ensure all newly added rules are valid PromQL expressions.

#### Unit tests
We will expose a new method within the TestServerInterface called
`MustGetMetricRule` which will return a metric rule with the given name. When an
engineer adds a new rule, they can add a unit test which initializes a server and
invokes this method to retrieve the rule and verify it. Some pseudocode to give
an idea of what this will look like:
```go
//Within testserver.go

// MustGetMetricRule implements TestServerInterface. This will
// complete in O(#rules) time which should be okay for the test
// framework.
func (ts *TestServer) MustGetMetricRule(name string) metric.Rule {
	// Scrape aggregated metric rules contained with RuleRegistry.
	rule := ts.ruleRegistry.GetRuleForTest(name)
	if rule != nil {
		return rule
	}
}

// Within a unit test.
func UnitTestMetricRule() {
	params, _ := tests.CreateTestServerParams()
	s := serverutils.StartServerOnly(t, params)
	defer s.Stopper().Stop(context.Background())
	rule = s.MustGetMetricRule("rule_name")
	require.NotNil(rule)
	// ...
}
```
#### Integration test
We will also add an integration test along the lines of the pseudocode below to
ensure newly added rules are valid rules.
```go
func IntegrationTestHandleRules() {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	server, err := NewServer(ctx, stopper, ...)
	require.NoError(t, err)

	rw := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v2/metrics/rules", nil)

	server.mux.ServeHTTP(rw, r)

	require.Equal(t, http.StatusOK, rw.Code)
	out, err := ioutil.ReadAll(rw.Body)
	require.NoError(t, err)
}
```

## Rationale and Alternatives
1. **Keeping the `expr` field more free form**: The idea here was to allow for `expr`
   data to be more flexible. However, keeping it aligned with the Prometheus format
   allows for us to use a structured format for specifying aggregation of metrics. It
   will also make the resulting rules more usable by our end users as there is no
  confusion on the format for specifying the expressions.

## Future Work
This RFC does not go over automated Grafana dashboard generation. The metric upgrade
discussed in this RFC is fairly complex, and it did not make much sense to add to the
scope creep by addressing Grafana dashboard generation. However, this framework can
be extended in the future for automated dashboard generation.
