// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// TopicDescriptor describes topic emitted by the sink.
type TopicDescriptor interface {
	// GetNameComponents returns the names that should go into any string-based topic identifier.
	// If the topic is a table, this will be the statement time name of the table, fully-qualified
	// if that option was set when creating the changefeed.
	GetNameComponents() []string
	// GetTopicIdentifier returns a struct suitable for use as a unique key in a map containing
	// all topics in a feed.
	GetTopicIdentifier() TopicIdentifier
	// GetVersion returns topic version.
	// For example, the underlying data source (e.g. table) may change, in which case
	// we may want to emit same Name/ID, but a different version number.
	GetVersion() descpb.DescriptorVersion
	// GetTargetSpecification() returns the target specification for this topic.
	// Currently this is assumed to be 1:1.
	GetTargetSpecification() jobspb.ChangefeedTargetSpecification
}

// TopicIdentifier is a minimal set of fields that
// uniquely identifies a topic.
type TopicIdentifier struct {
	TableID  descpb.ID
	FamilyID descpb.FamilyID
}

// TopicNamer generates and caches the strings used as topic keys by sinks,
// using target specifications, options, and sink-specific string manipulation.
type TopicNamer struct {
	join       byte
	prefix     string
	singleName string
	sanitize   func(string) string

	// DisplayNames are initialized once from specs and may contain placeholder strings.
	DisplayNames map[jobspb.ChangefeedTargetSpecification]string

	// FullNames are generated whenever Name() is actually called (usually during sink.EmitRow).
	// They do not contain placeholder strings.
	FullNames map[TopicIdentifier]string

	sliceCache []string
}

// MakeTopicNamer creates a TopicNamer.
// specs are used to populate DisplayNames and the values iterated over in Each.
// join is the separator for multi-part names, usually '.'.
// prefix will be prepended to all names.
// singleName if nonempty will override the normal logic and just return itself no
// matter what, although prefix and sanitize options will still be honored.
// sanitize if non-nil will be called to post-process the name.
func MakeTopicNamer(
	specs []jobspb.ChangefeedTargetSpecification,
	join byte,
	prefix string,
	singleName string,
	sanitize func(string) string,
) (*TopicNamer, error) {
	tn := TopicNamer{
		join:         join,
		prefix:       prefix,
		singleName:   singleName,
		sanitize:     sanitize,
		DisplayNames: make(map[jobspb.ChangefeedTargetSpecification]string, len(specs)),
		FullNames:    make(map[TopicIdentifier]string),
	}
	for _, s := range specs {
		name, err := tn.makeDisplayName(s)
		if err != nil {
			return nil, err
		}
		tn.DisplayNames[s] = name
	}

	return &tn, nil

}

const familyPlaceholder = "{family}"

// Name generates (with caching) a sink's topic identifier string.
func (tn *TopicNamer) Name(td TopicDescriptor) (string, error) {
	if name, ok := tn.FullNames[td.GetTopicIdentifier()]; ok {
		return name, nil
	}
	name, err := tn.makeName(td.GetTargetSpecification(), &td)
	tn.FullNames[td.GetTopicIdentifier()] = name
	return name, err
}

// DisplayNamesSlice gives all topics that are going to be emitted to,
// suitable for displaying to the user on feed creation.
func (tn *TopicNamer) DisplayNamesSlice() []string {
	if len(tn.sliceCache) > 0 {
		return tn.sliceCache
	}
	for _, n := range tn.DisplayNames {
		tn.sliceCache = append(tn.sliceCache, n)
		if tn.singleName != "" {
			return tn.sliceCache
		}
	}
	return tn.sliceCache
}

// Each is a convenience method that iterates a function over DisplayNamesSlice.
func (tn *TopicNamer) Each(fn func(string) error) error {
	for _, name := range tn.DisplayNames {
		err := fn(name)
		if tn.singleName != "" || err != nil {
			return err
		}
	}
	return nil
}

func (tn *TopicNamer) makeName(
	s jobspb.ChangefeedTargetSpecification, td *TopicDescriptor,
) (string, error) {
	switch s.Type {
	case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
		return tn.nameFromComponents(s.StatementTimeName), nil
	case jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY:
		return tn.nameFromComponents(s.StatementTimeName, s.FamilyName), nil
	case jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
		if td == nil {
			return tn.nameFromComponents(s.StatementTimeName, familyPlaceholder), nil
		}
		return tn.nameFromComponents((*td).GetNameComponents()...), nil
	default:
		return "", errors.AssertionFailedf("unrecognized type %s", s.Type)
	}
}

func (tn *TopicNamer) makeDisplayName(s jobspb.ChangefeedTargetSpecification) (string, error) {
	return tn.makeName(s, nil /* no topic descriptor yet, use placeholders if needed */)
}

func (tn *TopicNamer) nameFromComponents(components ...string) string {
	var b strings.Builder
	b.WriteString(tn.prefix)
	if tn.singleName != "" {
		b.WriteString(tn.singleName)
	} else {
		for i, c := range components {
			if i > 0 {
				b.WriteByte(tn.join)
			}
			b.WriteString(c)
		}
	}
	str := b.String()

	if tn.sanitize != nil {
		return tn.sanitize(str)
	}

	return str
}
