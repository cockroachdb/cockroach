// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package metric

import (
	"encoding/json"
	"io"
	"reflect"
	"regexp"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/gogo/protobuf/proto"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

const sep = "-"

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
			Name:  proto.String(name),
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
		log.Infof(context.TODO(), "Added metric: %s (%T)", metric.GetName(), metric)
	}
}

// AddMetricGroup expands the metric group and adds all of them
// as individual metrics to the registry.
func (r *Registry) AddMetricGroup(group metricGroup) {
	r.Lock()
	defer r.Unlock()
	group.iterate(func(metric Iterable) {
		r.tracked = append(r.tracked, metric)
		if log.V(2) {
			log.Infof(context.TODO(), "Added metric: %s (%T)", metric.GetName(), metric)
		}
	})
}

// AddMetricStruct examines all fields of metricStruct and adds
// all Iterable or metricGroup objects to the registry.
func (r *Registry) AddMetricStruct(metricStruct interface{}) {
	v := reflect.ValueOf(metricStruct)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		vfield, tfield := v.Field(i), t.Field(i)
		if !vfield.CanInterface() {
			if log.V(2) {
				log.Infof(context.TODO(), "Skipping unexported field %s", tfield.Name)
			}
			continue
		}
		val := vfield.Interface()
		switch typ := val.(type) {
		case metricGroup:
			r.AddMetricGroup(typ)
		case Iterable:
			r.AddMetric(typ)
		default:
			if log.V(2) {
				log.Infof(context.TODO(), "Skipping non-metric field %s", tfield.Name)
			}
		}
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
	nameReplaceRE = regexp.MustCompile("[.-]")
)

// exportedName takes a metric name and generates a valid prometheus name.
// see nameReplaceRE for characters to be replaces with '_'.
func exportedName(name string) string {
	return nameReplaceRE.ReplaceAllString(name, "_")
}

// PrintAsText outputs all metrics in text format.
func (r *Registry) PrintAsText(w io.Writer) error {
	var metricFamily prometheusgo.MetricFamily
	var ret error
	labels := r.getLabels()
	for _, metric := range r.tracked {
		metric.Inspect(func(v interface{}) {
			if ret != nil {
				return
			}
			if prom, ok := v.(PrometheusExportable); ok {
				metricFamily.Reset()
				metricFamily.Name = proto.String(exportedName(metric.GetName()))
				metricFamily.Help = proto.String(exportedName(metric.GetHelp()))
				prom.FillPrometheusMetric(&metricFamily)
				if len(labels) != 0 {
					// Set labels. We only set one metric in the slice, but loop anyway.
					for _, m := range metricFamily.Metric {
						m.Label = labels
					}
				}
				if _, err := expfmt.MetricFamilyToText(w, &metricFamily); err != nil {
					ret = err
				}
			}
		})
	}
	return ret
}
