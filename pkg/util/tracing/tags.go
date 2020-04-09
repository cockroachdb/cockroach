// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"context"

	"github.com/cockroachdb/logtags"
	opentracing "github.com/opentracing/opentracing-go"
)

// LogTagsOption is a StartSpanOption that uses log tags to populate the span tags.
type logTagsOption logtags.Buffer

var _ opentracing.StartSpanOption = &logTagsOption{}

// Apply is part of the opentracing.StartSpanOption interface.
//
// The tags in the buffer go through the log tag -> span tag remapping (see
// tagName()).
//
// Note that our tracer does not call Apply() for this options. Instead, it
// recognizes it as a special case and treats it more efficiently, avoiding
// allocations for each tag. Apply() is still used by shadow tracers.
func (lt *logTagsOption) Apply(o *opentracing.StartSpanOptions) {
	if lt == nil {
		return
	}
	tags := (*logtags.Buffer)(lt).Get()
	if len(tags) == 0 {
		return
	}
	if o.Tags == nil {
		o.Tags = make(map[string]interface{}, len(tags))
	}
	for i := range tags {
		o.Tags[tagName(tags[i].Key())] = tags[i].ValueStr()
	}
}

// LogTags returns a StartSpanOption that sets the span tags to the given log
// tags. When applied, the returned option will apply any logtag name->span tag
// name remapping that has been registered via RegisterTagRemapping.
func LogTags(tags *logtags.Buffer) opentracing.StartSpanOption {
	return (*logTagsOption)(tags)
}

// LogTagsFromCtx returns a StartSpanOption that sets the span tags to the log
// tags in the context.
func LogTagsFromCtx(ctx context.Context) opentracing.StartSpanOption {
	return (*logTagsOption)(logtags.FromContext(ctx))
}

// tagRemap is a map that records desired conversions
var tagRemap = make(map[string]string)

// RegisterTagRemapping sets the span tag name that corresponds to the given log
// tag name. Should be called as part of an init() function.
func RegisterTagRemapping(logTag, spanTag string) {
	tagRemap[logTag] = spanTag
}

func tagName(logTag string) string {
	if v, ok := tagRemap[logTag]; ok {
		return v
	}
	return logTag
}
