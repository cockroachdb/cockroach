// Copyright 2018 The Cockroach Authors.
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

package tracing

import (
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// This file provides functionality to structure the storage and retrieval of
// span-level stats from trace tags.
// Components that would like to store span-level stats should register a
// SpanStatDescriptor through RegisterStat.
// Consumers of a trace with span tags may then use all tags prefixed with
// statPrefix to get information on the associated stat.

// statPrefix is a prefix used on
const statPrefix = "stat."

// SpanStatDescriptor describes a stat that could be stored in a span's tags.
type SpanStatDescriptor struct {
	Name string
	Help string
	// Render, given a string value for the stat, returns a display-friendly
	// string.
	Render func(string) string
}

// spanStatRegistry keeps track of all registered SpanStatDescriptors.
var spanStatRegistry = make(map[string]SpanStatDescriptor)

// RegisterStat adds stat to the registry.
func RegisterStat(stat SpanStatDescriptor) {
	spanStatRegistry[stat.Name] = stat
}

// GetStat returns the SpanStatDescriptor associated with name. nil, false is
// returned if a stat with that name has not been registered.
func GetStat(name string) (SpanStatDescriptor, bool) {
	if strings.HasPrefix(name, statPrefix) {
		name = name[len(statPrefix):]
	}
	s, ok := spanStatRegistry[name]
	return s, ok
}

// SetStat checks the registry for the existence of a SpanStatDescriptor
// associated with name and adds a tag to the opentracing span, overwriting the
// tag if one already exists with that name.
func SetStat(os opentracing.Span, name string, value interface{}) error {
	if _, ok := spanStatRegistry[name]; !ok {
		return errors.New("adding unregistered stat")
	}
	if !strings.HasPrefix(name, statPrefix) {
		name = statPrefix + name
	}
	os.SetTag(name, value)
	return nil
}

//TODO(asubiotto): Add tests.
