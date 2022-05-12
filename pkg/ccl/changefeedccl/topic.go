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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
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
	// Currently this is assumed to be 1:1, or to be many: 1 for EachColumnFamily topics.
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

// TopicNameOption is an optional argument to MakeTopicNamer.
type TopicNameOption interface {
	set(*TopicNamer)
}

type optJoinByte byte

func (o optJoinByte) set(tn *TopicNamer) {
	tn.join = byte(o)
}

// WithJoinByte overrides the default '.' separator between a table and family name.
func WithJoinByte(b byte) TopicNameOption {
	return optJoinByte(b)
}

type optPrefix string

func (o optPrefix) set(tn *TopicNamer) {
	tn.prefix = string(o)
}

// WithPrefix defines a prefix string for all topics named by this TopicNamer.
func WithPrefix(s string) TopicNameOption {
	return optPrefix(s)
}

type optSingleName string

func (o optSingleName) set(tn *TopicNamer) {
	tn.singleName = string(o)
}

// WithSingleName causes all topics named by this TopicNamer to be the same,
// overriding things like the table name but not options like WithPrefix.
func WithSingleName(s string) TopicNameOption {
	return optSingleName(s)
}

type optSanitize func(string) string

func (o optSanitize) set(tn *TopicNamer) {
	tn.sanitize = o
}

// WithSanitizeFn defines a post-processor for all topic names.
func WithSanitizeFn(fn func(string) string) TopicNameOption {
	return optSanitize(fn)
}

// MakeTopicNamer creates a TopicNamer.
// specs are used to populate DisplayNames and the values iterated over in Each.
// Add options using WithJoinByte, WithPrefix, WithSingleName, and/or WithSanitizeFn.
func MakeTopicNamer(
	specs []jobspb.ChangefeedTargetSpecification, opts ...TopicNameOption,
) (*TopicNamer, error) {
	tn := &TopicNamer{
		join:         '.',
		DisplayNames: make(map[jobspb.ChangefeedTargetSpecification]string, len(specs)),
		FullNames:    make(map[TopicIdentifier]string),
	}
	for _, opt := range opts {
		opt.set(tn)
	}
	for _, s := range specs {
		name, err := tn.makeDisplayName(s)
		if err != nil {
			return nil, err
		}
		tn.DisplayNames[s] = name
	}

	return tn, nil

}

const familyPlaceholder = "{family}"

// Name generates (with caching) a sink's topic identifier string.
func (tn *TopicNamer) Name(td TopicDescriptor) (string, error) {
	if name, ok := tn.FullNames[td.GetTopicIdentifier()]; ok {
		return name, nil
	}
	name, err := tn.makeName(td.GetTargetSpecification(), td)
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

// A nil topic descriptor means we're building solely from the spec
// and should use placeholders if necessary. Only necessary in the
// EACH_FAMILY case as in the COLUMN_FAMILY case we know the name from
// the spec.
func (tn *TopicNamer) makeName(
	s jobspb.ChangefeedTargetSpecification, td TopicDescriptor,
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
		return tn.nameFromComponents(td.GetNameComponents()...), nil
	default:
		return "", errors.AssertionFailedf("unrecognized type %s", s.Type)
	}
}

func (tn *TopicNamer) makeDisplayName(s jobspb.ChangefeedTargetSpecification) (string, error) {
	return tn.makeName(s, nil /* no topic descriptor yet, use placeholders if needed */)
}

func (tn *TopicNamer) nameFromComponents(components ...string) string {
	// Use strings.Builder rather than strings.Join because the join
	// character isn't used with the prefix, so we save a string copy this way.
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

type tableDescriptorTopic struct {
	cdcevent.Metadata
	spec                jobspb.ChangefeedTargetSpecification
	nameComponentsCache []string
	identifierCache     TopicIdentifier
}

// GetNameComponents implements the TopicDescriptor interface
func (tdt *tableDescriptorTopic) GetNameComponents() []string {
	if len(tdt.nameComponentsCache) == 0 {
		tdt.nameComponentsCache = []string{tdt.spec.StatementTimeName}
	}
	return tdt.nameComponentsCache
}

// GetTopicIdentifier implements the TopicDescriptor interface
func (tdt *tableDescriptorTopic) GetTopicIdentifier() TopicIdentifier {
	if tdt.identifierCache.TableID == 0 {
		tdt.identifierCache = TopicIdentifier{
			TableID: tdt.TableID,
		}
	}
	return tdt.identifierCache
}

// GetVersion implements the TopicDescriptor interface
func (tdt *tableDescriptorTopic) GetVersion() descpb.DescriptorVersion {
	return tdt.Version
}

// GetTargetSpecification implements the TopicDescriptor interface
func (tdt *tableDescriptorTopic) GetTargetSpecification() jobspb.ChangefeedTargetSpecification {
	return tdt.spec
}

var _ TopicDescriptor = &tableDescriptorTopic{}

type columnFamilyTopic struct {
	cdcevent.Metadata
	spec                jobspb.ChangefeedTargetSpecification
	nameComponentsCache []string
	identifierCache     TopicIdentifier
}

// GetNameComponents implements the TopicDescriptor interface
func (cft *columnFamilyTopic) GetNameComponents() []string {
	if len(cft.nameComponentsCache) == 0 {
		cft.nameComponentsCache = []string{
			cft.spec.StatementTimeName,
			cft.FamilyName,
		}
	}
	return cft.nameComponentsCache
}

// GetTopicIdentifier implements the TopicDescriptor interface
func (cft *columnFamilyTopic) GetTopicIdentifier() TopicIdentifier {
	if cft.identifierCache.TableID == 0 {
		cft.identifierCache = TopicIdentifier{
			TableID:  cft.TableID,
			FamilyID: cft.FamilyID,
		}
	}
	return cft.identifierCache
}

// GetVersion implements the TopicDescriptor interface
func (cft *columnFamilyTopic) GetVersion() descpb.DescriptorVersion {
	return cft.Version
}

// GetTargetSpecification implements the TopicDescriptor interface
func (cft *columnFamilyTopic) GetTargetSpecification() jobspb.ChangefeedTargetSpecification {
	return cft.spec
}

var _ TopicDescriptor = &columnFamilyTopic{}

type noTopic struct{}

func (n noTopic) GetNameComponents() []string {
	return []string{}
}

func (n noTopic) GetTopicIdentifier() TopicIdentifier {
	return TopicIdentifier{}
}

func (n noTopic) GetVersion() descpb.DescriptorVersion {
	return 0
}

func (n noTopic) GetTargetSpecification() jobspb.ChangefeedTargetSpecification {
	return jobspb.ChangefeedTargetSpecification{}
}

var _ TopicDescriptor = &noTopic{}

func makeTopicDescriptorFromSpec(
	s jobspb.ChangefeedTargetSpecification, src cdcevent.Metadata,
) (TopicDescriptor, error) {
	switch s.Type {
	case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
		return &tableDescriptorTopic{
			Metadata: src,
			spec:     s,
		}, nil
	case jobspb.ChangefeedTargetSpecification_EACH_FAMILY, jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY:
		return &columnFamilyTopic{
			Metadata: src,
			spec:     s,
		}, nil
	default:
		return noTopic{}, errors.AssertionFailedf("Unsupported target type %s", s.Type)
	}
}
