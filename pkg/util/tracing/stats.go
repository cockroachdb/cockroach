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

	"fmt"

	"github.com/opentracing/opentracing-go"
)

// This file provides functionality to structure the storage and retrieval of
// span-level stats from trace tags.
// Components that would like to store span-level stats should register a
// SpanStatDescriptor through RegisterSpanStat.
// Consumers of a trace containing span stats as tags may use
// GetSpanStatsFromTags on a span to filter out all non-stat span tags and
// look the stat names up in the registry for more information on these stats.

// SpanStatPrefix is a prefix used for the span tags that are stats.
const SpanStatPrefix = "cockroach.stat."

// SpanStatDescriptor describes a stat that could be stored in a span's tags.
type SpanStatDescriptor interface {
	GetName() string
	GetHelp() string
	// Render, given a string value for the stat, returns a display-friendly
	// string.
	Render(string) string
}

// SpanStatDescriptorBase is a base implementation of a SpanStatDescriptor.
// Other SpanStatDescriptor implementations can embed it and redefine Render.
type SpanStatDescriptorBase struct {
	Name string
	Help string
}

// GetName is part of the SpanStatDescriptor interface.
func (sd SpanStatDescriptorBase) GetName() string {
	return sd.Name
}

// GetHelp is part of the SpanStatDescriptor interface.
func (sd SpanStatDescriptorBase) GetHelp() string {
	return sd.Help
}

// Render is part of the SpanStatDescriptor interface.
func (sd SpanStatDescriptorBase) Render(s string) string {
	return s
}

// spanStatRegistry keeps track of all registered SpanStatDescriptors.
var spanStatRegistry = make(map[string]SpanStatDescriptor)

// RegisterSpanStat adds stat to the registry, panicking if one already exists
// with a matching name. Must only be called at init() time.
func RegisterSpanStat(stat SpanStatDescriptor) {
	if _, ok := spanStatRegistry[stat.GetName()]; ok {
		panic(fmt.Sprintf("stat already registered: %s", stat.GetName()))
	}
	spanStatRegistry[stat.GetName()] = stat
}

// LookupSpanStat returns the SpanStatDescriptor associated with name.
// (nil, false) is returned if a stat with that name has not been registered.
func LookupSpanStat(name string) (SpanStatDescriptor, bool) {
	s, ok := spanStatRegistry[strings.TrimPrefix(name, SpanStatPrefix)]
	return s, ok
}

// SetSpanStat adds a tag to the opentracing span, overwriting the tag if one
// already exists with that name. A SpanStatDescriptor with a matching name must
// have been previously registered through RegisterSpanStat.
func SetSpanStat(os opentracing.Span, name string, value interface{}) {
	if _, ok := spanStatRegistry[name]; !ok {
		panic(fmt.Sprintf("adding unregistered stat: %s", name))
	}
	os.SetTag(SpanStatPrefix+name, value)
}

// GetSpanStatsFromTags removes SpanStatPrefix from all span stats found in tags
// and returns this subset with their associated value.
func GetSpanStatsFromTags(tags map[string]string) map[string]string {
	result := make(map[string]string)
	for name, value := range tags {
		if !strings.HasPrefix(name, SpanStatPrefix) {
			continue
		}
		result[name[len(SpanStatPrefix):]] = value
	}
	return result
}
