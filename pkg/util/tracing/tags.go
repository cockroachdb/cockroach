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

import "github.com/cockroachdb/logtags"

// LogTagsOption is a StartSpanOption that uses log tags to populate the Span tags.
type logTagsOption logtags.Buffer

var _ SpanOption = &logTagsOption{}

func (lt *logTagsOption) apply(opts spanOptions) spanOptions {
	opts.LogTags = (*logtags.Buffer)(lt)
	return opts
}

// WithLogTags returns a SpanOption that sets the Span tags to the given log
// tags. When applied, the returned option will apply any logtag name->Span tag
// name remapping that has been registered via RegisterTagRemapping.
//
// Note that there is no need to use this option with StartSpanCtx, as that will
// already propagate the log tags from the Context supplied to it to the Span.
// However, if a WithLogTags option is supplied, it will be used and replaces
// the Context-derived tags.
func WithLogTags(tags *logtags.Buffer) SpanOption {
	return (*logTagsOption)(tags)
}

// tagRemap is a map that records desired conversions
var tagRemap = make(map[string]string)

// RegisterTagRemapping sets the Span tag name that corresponds to the given log
// tag name. Should be called as part of an init() function.
func RegisterTagRemapping(logTag, spanTag string) {
	tagRemap[logTag] = spanTag
}

// setLogTags calls the provided function for each tag pair from the provided log tags.
// It takes into account any prior calls to RegisterTagRemapping.
func setLogTags(logTags []logtags.Tag, setTag func(remappedKey string, value *logtags.Tag)) {
	tagName := func(logTag string) string {
		if v, ok := tagRemap[logTag]; ok {
			return v
		}
		return logTag
	}

	for i := range logTags {
		tag := &logTags[i]
		setTag(tagName(tag.Key()), tag)
	}
}
