// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// TopicDescriptor describes topic emitted by the sink.
type TopicDescriptor interface {
	// GetNameComponents returns the names that should go into any string-based topic identifier.
	// If the topic is a table, this will be the statement time name of the table, fully-qualified
	// if that option was set when creating the changefeed.
	GetNameComponents() (changefeedbase.StatementTimeName, []string)
	// GetTopicIdentifier returns a struct suitable for use as a unique key in a map containing
	// all topics in a feed.
	GetTopicIdentifier() TopicIdentifier
	// GetVersion returns topic version.
	// For example, the underlying data source (e.g. table) may change, in which case
	// we may want to emit same Name/ID, but a different version number.
	GetVersion() descpb.DescriptorVersion
	// GetTargetSpecification() returns the target specification for this topic.
	// Currently this is assumed to be 1:1, or to be many: 1 for EachColumnFamily topics.
	GetTargetSpecification() changefeedbase.Target
	// GetTableName returns the table name for the row attached to this event.
	GetTableName() string
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
	DisplayNames map[changefeedbase.Target]string

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
func MakeTopicNamer(targets changefeedbase.Targets, opts ...TopicNameOption) (*TopicNamer, error) {
	tn := &TopicNamer{
		join:         '.',
		DisplayNames: make(map[changefeedbase.Target]string, targets.Size),
		FullNames:    make(map[TopicIdentifier]string),
	}
	for _, opt := range opts {
		opt.set(tn)
	}
	err := targets.EachTarget(func(t changefeedbase.Target) error {
		name, err := tn.makeDisplayName(t)
		if err != nil {
			return err
		}
		tn.DisplayNames[t] = name
		return nil
	})

	return tn, err

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
func (tn *TopicNamer) makeName(s changefeedbase.Target, td TopicDescriptor) (string, error) {
	switch s.Type {
	case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
		return tn.nameFromComponents(s.StatementTimeName), nil
	case jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY:
		return tn.nameFromComponents(s.StatementTimeName, s.FamilyName), nil
	case jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
		if td == nil {
			return tn.nameFromComponents(s.StatementTimeName, familyPlaceholder), nil
		}
		name, components := td.GetNameComponents()
		return tn.nameFromComponents(name, components...), nil
	default:
		return "", errors.AssertionFailedf("unrecognized type %s", s.Type)
	}
}

func (tn *TopicNamer) makeDisplayName(s changefeedbase.Target) (string, error) {
	return tn.makeName(s, nil /* no topic descriptor yet, use placeholders if needed */)
}

func (tn *TopicNamer) nameFromComponents(
	name changefeedbase.StatementTimeName, components ...string,
) string {
	// Use strings.Builder rather than strings.Join because the join
	// character isn't used with the prefix, so we save a string copy this way.
	var b strings.Builder
	b.WriteString(tn.prefix)
	if tn.singleName != "" {
		b.WriteString(tn.singleName)
	} else {
		b.WriteString(string(name))
		for _, c := range components {
			b.WriteByte(tn.join)
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
	spec            changefeedbase.Target
	identifierCache TopicIdentifier
}

// GetNameComponents implements the TopicDescriptor interface
func (tdt *tableDescriptorTopic) GetNameComponents() (changefeedbase.StatementTimeName, []string) {
	return tdt.spec.StatementTimeName, []string{}
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
func (tdt *tableDescriptorTopic) GetTargetSpecification() changefeedbase.Target {
	return tdt.spec
}

// GetTableName implements the TopicDescriptor interface
func (tdt *tableDescriptorTopic) GetTableName() string {
	return tdt.TableName
}

var _ TopicDescriptor = &tableDescriptorTopic{}

type columnFamilyTopic struct {
	cdcevent.Metadata
	spec            changefeedbase.Target
	identifierCache TopicIdentifier
}

// GetNameComponents implements the TopicDescriptor interface
func (cft *columnFamilyTopic) GetNameComponents() (changefeedbase.StatementTimeName, []string) {
	return cft.spec.StatementTimeName, []string{cft.FamilyName}
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
func (cft *columnFamilyTopic) GetTargetSpecification() changefeedbase.Target {
	return cft.spec
}

// GetTableName implements the TopicDescriptor interface
func (cft *columnFamilyTopic) GetTableName() string {
	return cft.TableName
}

var _ TopicDescriptor = &columnFamilyTopic{}

type noTopic struct{}

var noStatementTimeName changefeedbase.StatementTimeName = ""

func (n noTopic) GetNameComponents() (changefeedbase.StatementTimeName, []string) {
	return noStatementTimeName, []string{}
}

func (n noTopic) GetTopicIdentifier() TopicIdentifier {
	return TopicIdentifier{}
}

func (n noTopic) GetVersion() descpb.DescriptorVersion {
	return 0
}

func (n noTopic) GetTargetSpecification() changefeedbase.Target {
	return changefeedbase.Target{}
}

// GetTableName implements the TopicDescriptor interface
func (n noTopic) GetTableName() string {
	return ""
}

var _ TopicDescriptor = &noTopic{}

func makeTopicDescriptorFromSpec(
	s changefeedbase.Target, src cdcevent.Metadata,
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
