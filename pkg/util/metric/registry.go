// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"

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
	labels  []*prometheusgo.LabelPair
	tracked []Iterable
}

// Struct can be implemented by the types of members of a metric
// container so that the members get automatically registered.
type Struct interface {
	MetricStruct()
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{
		labels:  []*prometheusgo.LabelPair{},
		tracked: []Iterable{},
	}
}

// AddLabel adds a label/value pair for this registry.
func (r *Registry) AddLabel(name, value string) {
	r.Lock()
	defer r.Unlock()
	r.labels = append(r.labels,
		&prometheusgo.LabelPair{
			Name:  proto.String(exportedLabel(name)),
			Value: proto.String(value),
		})
}

func (r *Registry) getLabels() []*prometheusgo.LabelPair {
	r.Lock()
	defer r.Unlock()
	return r.labels
}

// AddMetric adds the passed-in metric to the registry.
func (r *Registry) AddMetric(metric Iterable) {
	r.Lock()
	defer r.Unlock()
	r.tracked = append(r.tracked, metric)
	if log.V(2) {
		log.Infof(context.TODO(), "added metric: %s (%T)", metric.GetName(), metric)
	}
}

// AddMetricStruct examines all fields of metricStruct and adds
// all Iterable or metric.Struct objects to the registry.
func (r *Registry) AddMetricStruct(metricStruct interface{}) {
	ctx := context.TODO()
	v := reflect.ValueOf(metricStruct)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		vfield, tfield := v.Field(i), t.Field(i)
		tname := tfield.Name
		if !vfield.CanInterface() {
			if log.V(2) {
				log.Infof(ctx, "skipping unexported field %s", tname)
			}
			continue
		}
		switch vfield.Kind() {
		case reflect.Array:
			for i := 0; i < vfield.Len(); i++ {
				velem := vfield.Index(i)
				telemName := fmt.Sprintf("%s[%d]", tname, i)
				// Permit elements in the array to be nil.
				const skipNil = true
				r.addMetricValue(ctx, velem, telemName, skipNil)
			}
		default:
			// No metric fields should be nil.
			const skipNil = false
			r.addMetricValue(ctx, vfield, tname, skipNil)
		}
	}
}

func (r *Registry) addMetricValue(
	ctx context.Context, val reflect.Value, name string, skipNil bool,
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
		if log.V(2) {
			log.Infof(ctx, "skipping non-metric field %s", name)
		}
	}
}

// WriteMetricsMetadata writes metadata from all tracked metrics to the
// parameter map.
func (r *Registry) WriteMetricsMetadata(dest map[string]Metadata) {
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

// MarshalJSON marshals to JSON.
func (r *Registry) MarshalJSON() ([]byte, error) {
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
