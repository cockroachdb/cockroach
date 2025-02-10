// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/gogo/protobuf/proto"
	prometheusgo "github.com/prometheus/client_model/go"
)

// A Registry is a list of metrics. It provides a simple way of iterating over
// them, can marshal into JSON, and generate a prometheus format.
//
// A registry can have label pairs that will be applied to all its metrics
// when exported to prometheus.
type Registry struct {
	syncutil.Mutex
	labels  []labelPair
	tracked map[string]Iterable

	// computedLabels get filled in by GetLabels().
	// We hold onto the slice to avoid a re-allocation every
	// time the metrics get scraped.
	computedLabels []*prometheusgo.LabelPair
}

type labelPair struct {
	name  string
	value interface{}
}

// Struct can be implemented by the types of members of a metric
// container so that the members get automatically registered.
type Struct interface {
	MetricStruct()
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{
		labels:         []labelPair{},
		computedLabels: []*prometheusgo.LabelPair{},
		tracked:        map[string]Iterable{},
	}
}

// AddLabel adds a label/value pair for this registry.
func (r *Registry) AddLabel(name string, value interface{}) {
	r.Lock()
	defer r.Unlock()
	r.labels = append(r.labels, labelPair{name: exportedLabel(name), value: value})
	r.computedLabels = append(r.computedLabels, &prometheusgo.LabelPair{})
}

func (r *Registry) GetLabels() []*prometheusgo.LabelPair {
	r.Lock()
	defer r.Unlock()
	for i, l := range r.labels {
		r.computedLabels[i].Name = proto.String(l.name)
		r.computedLabels[i].Value = proto.String(fmt.Sprint(l.value))
	}
	return r.computedLabels
}

// AddMetric adds the passed-in metric to the registry.
func (r *Registry) AddMetric(metric Iterable) {
	r.Lock()
	defer r.Unlock()
	r.tracked[metric.GetName()] = metric
	if log.V(2) {
		log.Infof(context.TODO(), "added metric: %s (%T)", metric.GetName(), metric)
	}
}

// Contains returns true if the given metric name is registered in this
// registry. The method will automatically trim `cr.store` and
// `cr.node` prefixes if they're present in the name, otherwise leaving
// the input unchanged.
func (r *Registry) Contains(name string) bool {
	r.Lock()
	defer r.Unlock()

	name = strings.TrimPrefix(name, "cr.store.")
	name = strings.TrimPrefix(name, "cr.node.")
	_, ok := r.tracked[name]
	return ok
}

// RemoveMetric removes the passed-in metric from the registry. If the metric
// does not exist, this is a no-op.
func (r *Registry) RemoveMetric(metric Iterable) {
	r.Lock()
	defer r.Unlock()
	delete(r.tracked, metric.GetName())
	if log.V(2) {
		log.Infof(context.TODO(), "removed metric: %s (%T)", metric.GetName(), metric)
	}
}

// AddMetricStruct examines all fields of metricStruct and adds
// all Iterable or metric.Struct objects to the registry.
func (r *Registry) AddMetricStruct(metricStruct interface{}) {
	if r == nil { // for testing convenience
		return
	}

	ctx := context.TODO()
	v := reflect.ValueOf(metricStruct)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()

	const allowNil = true
	const disallowNil = false
	for i := 0; i < v.NumField(); i++ {
		vfield, tfield := v.Field(i), t.Field(i)
		tname := tfield.Name

		if !vfield.CanInterface() {
			checkFieldCanBeSkipped("unexported", tname, tfield.Type, t)
			continue
		}

		switch vfield.Kind() {
		case reflect.Array, reflect.Slice:
			for i := 0; i < vfield.Len(); i++ {
				velem := vfield.Index(i)
				telemName := fmt.Sprintf("%s[%d]", tname, i)
				r.addMetricValue(ctx, velem, telemName, allowNil, t)
			}
		case reflect.Map:
			iter := vfield.MapRange()
			for iter.Next() {
				// telemName is only used for assertion errors.
				telemName := iter.Key().String()
				r.addMetricValue(ctx, iter.Value(), telemName, allowNil, t)
			}
		default:
			// No metric fields should be nil.
			r.addMetricValue(ctx, vfield, tname, disallowNil, t)
		}
	}
}

func (r *Registry) addMetricValue(
	ctx context.Context, val reflect.Value, name string, skipNil bool, parentType reflect.Type,
) {
	if val.Kind() == reflect.Ptr && val.IsNil() {
		if skipNil {
			if log.V(2) {
				log.Infof(ctx, "skipping nil metric field %s", name)
			}
		} else {
			log.Fatalf(ctx, "found nil metric field %s", name)
		}
		return
	}
	switch typ := val.Interface().(type) {
	case Iterable:
		r.AddMetric(typ)
	case Struct:
		r.AddMetricStruct(typ)
	default:
		checkFieldCanBeSkipped("non-metric", name, val.Type(), parentType)
	}
}

// WriteMetricsMetadata writes metadata from all tracked metrics to the
// parameter map.
func (r *Registry) WriteMetricsMetadata(dest map[string]Metadata) {
	r.Lock()
	defer r.Unlock()
	for _, v := range r.tracked {
		dest[v.GetName()] = v.GetMetadata()
	}
}

// Each calls the given closure for all metrics.
func (r *Registry) Each(f func(name string, val interface{})) {
	r.Lock()
	defer r.Unlock()
	for _, metric := range r.tracked {
		metric.Inspect(func(v interface{}) {
			f(metric.GetName(), v)
		})
	}
}

// Select calls the given closure for the selected metric names.
func (r *Registry) Select(metrics map[string]struct{}, f func(name string, val interface{})) {
	r.Lock()
	defer r.Unlock()
	for name := range metrics {
		metric, found := r.tracked[name]
		if found {
			metric.Inspect(func(v interface{}) {
				f(name, v)
			})
		}
	}
}

// MarshalJSON marshals to JSON.
func (r *Registry) MarshalJSON() ([]byte, error) {
	r.Lock()
	defer r.Unlock()
	m := make(map[string]interface{})
	for _, metric := range r.tracked {
		metric.Inspect(func(v interface{}) {
			m[metric.GetName()] = v
		})
	}
	return json.Marshal(m)
}

var (
	// Prometheus metric names and labels have fairly strict rules, they
	// must match the regexp [a-zA-Z_:][a-zA-Z0-9_:]*
	// See: https://prometheus.io/docs/concepts/data_model/
	prometheusNameReplaceRE  = regexp.MustCompile("^[^a-zA-Z_:]|[^a-zA-Z0-9_:]")
	prometheusLabelReplaceRE = regexp.MustCompile("^[^a-zA-Z_]|[^a-zA-Z0-9_]")
)

// exportedName takes a metric name and generates a valid prometheus name.
func exportedName(name string) string {
	return prometheusNameReplaceRE.ReplaceAllString(name, "_")
}

// exportedLabel takes a metric name and generates a valid prometheus name.
func exportedLabel(name string) string {
	return prometheusLabelReplaceRE.ReplaceAllString(name, "_")
}

var panicHandler = log.Fatalf

func testingSetPanicHandler(h func(ctx context.Context, msg string, args ...interface{})) func() {
	panicHandler = h
	return func() { panicHandler = log.Fatalf }
}

// checkFieldCanBeSkipped detects common mis-use patterns with metrics registry
// when running under test.
func checkFieldCanBeSkipped(
	skipReason, fieldName string, fieldType reflect.Type, parentType reflect.Type,
) {
	if !buildutil.CrdbTestBuild {
		if log.V(2) {
			log.Infof(context.Background(), "skipping %s field %s", skipReason, fieldName)
		}
		return
	}

	qualifiedFieldName := func() string {
		if parentType == nil {
			return fieldName
		}
		parentName := parentType.Name()
		if parentName == "" {
			parentName = "<unnamed>"
		}
		return fmt.Sprintf("%s.%s", parentName, fieldName)
	}()

	if isMetricType(fieldType) {
		panicHandler(context.Background(),
			"metric field %s (or any of embedded metrics) must be exported", qualifiedFieldName)
	}

	switch fieldType.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map:
		checkFieldCanBeSkipped(skipReason, fieldName, fieldType.Elem(), parentType)
	case reflect.Struct:
		containsMetrics := false
		for i := 0; i < fieldType.NumField() && !containsMetrics; i++ {
			containsMetrics = containsMetricType(fieldType.Field(i).Type)
		}

		if containsMetrics {
			// Embedded struct contains metrics.  Make sure the struct implements
			// metric.Struct interface.
			_, isStruct := reflect.New(fieldType).Interface().(Struct)
			if !isStruct {
				panicHandler(context.Background(),
					"embedded struct field %s (%s) does not implement metric.Struct interface", qualifiedFieldName, fieldType)
			}
		}
	}
}

// isMetricType returns true if the type is one of the metric type interfaces.
func isMetricType(ft reflect.Type) bool {
	v := reflect.Zero(ft)
	if !v.CanInterface() {
		return false
	}

	switch v.Interface().(type) {
	case Iterable, Struct:
		return true
	default:
		return false
	}
}

// containsMetricTypes returns true if the type or any types inside aggregate
// type (array, struct, etc) is metric type.
func containsMetricType(ft reflect.Type) bool {
	if isMetricType(ft) {
		return true
	}

	switch ft.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return containsMetricType(ft.Elem())
	case reflect.Struct:
		for i := 0; i < ft.NumField(); i++ {
			if containsMetricType(ft.Field(i).Type) {
				return true
			}
		}
		return false
	default:
		return false
	}
}
