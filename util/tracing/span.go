// Copyright 2016 The Cockroach Authors.
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

package tracing

import basictracer "github.com/opentracing/basictracer-go"

var _ basictracer.DelegatingCarrier = &Span{}

// SetState implements basictracer.DelegatingCarrier.
func (sp *Span) SetState(traceID, spanID uint64, sampled bool) {
	sp.TraceID = traceID
	sp.SpanID = spanID
	sp.Sampled = sampled
}

// State implements basictracer.DelegatingCarrier.
func (sp *Span) State() (traceID, spanID uint64, sampled bool) {
	return sp.TraceID, sp.SpanID, sp.Sampled
}

// SetBaggageItem implements basictracer.DelegatingCarrier.
func (sp *Span) SetBaggageItem(key, value string) {
	if sp.Baggage == nil {
		sp.Baggage = make(map[string]string)
	}
	sp.Baggage[key] = value
}

// GetBaggage implements basictracer.DelegatingCarrier.
func (sp *Span) GetBaggage(fn func(key, value string)) {
	for k, v := range sp.Baggage {
		fn(k, v)
	}
}
