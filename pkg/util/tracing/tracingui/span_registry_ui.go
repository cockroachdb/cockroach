// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingui

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// This file has helpers for the RPCs done by the #/debug/tracez page, which
// lists a snapshot of the spans in the Tracer's active spans registry.

// ProcessSnapshot massages a trace snapshot to prepare it for presentation in
// the UI.
func ProcessSnapshot(
	snapshot tracing.SpansSnapshot, registry *tracing.SpanRegistry,
) *ProcessedSnapshot {
	// Build a map of current spans.
	currentSpans := make(map[tracingpb.SpanID]tracingpb.RecordingType, 1000)
	registry.VisitSpans(func(sp tracing.RegistrySpan) {
		currentSpans[sp.SpanID()] = sp.RecordingType()
	})

	// Flatten the recordings.
	spans := make([]tracingpb.RecordedSpan, 0, len(snapshot.Traces)*3)
	for _, r := range snapshot.Traces {
		spans = append(spans, r...)
	}

	spansMap := make(map[uint64]*processedSpan)
	childrenMap := make(map[uint64][]*processedSpan)
	processedSpans := make([]processedSpan, len(spans))
	for i, s := range spans {
		p := processSpan(s, snapshot)
		p.CurrentRecordingMode, p.Current = currentSpans[s.SpanID]
		ptr := &processedSpans[i]
		*ptr = p
		spansMap[p.SpanID] = &processedSpans[i]
		if _, ok := childrenMap[p.ParentSpanID]; !ok {
			childrenMap[p.ParentSpanID] = []*processedSpan{&processedSpans[i]}
		} else {
			childrenMap[p.ParentSpanID] = append(childrenMap[p.ParentSpanID], &processedSpans[i])
		}
	}
	// Propagate tags up.
	for _, s := range processedSpans {
		for _, t := range s.Tags {
			if !t.PropagateUp || t.CopiedFromChild {
				continue
			}
			propagateTagUpwards(t, &s, spansMap)
		}
	}
	// Propagate tags down.
	for _, s := range processedSpans {
		for _, t := range s.Tags {
			if !t.Inherit || t.Inherited {
				continue
			}
			propagateInheritTagDownwards(t, &s, childrenMap)
		}
	}

	// Copy the stack traces and augment the map.
	stacks := make(map[int]string, len(snapshot.Stacks))
	for k, v := range snapshot.Stacks {
		stacks[k] = v
	}
	// Fill in messages for the goroutines for which we don't have a stack trace.
	for _, s := range spans {
		gid := int(s.GoroutineID)
		if _, ok := stacks[gid]; !ok {
			stacks[gid] = "Goroutine not found. Goroutine must have finished since the span was created."
		}
	}
	return &ProcessedSnapshot{
		Spans:  processedSpans,
		Stacks: stacks,
	}
}

// ProcessedSnapshot represents a snapshot of open tracing spans plus stack
// traces for all the goroutines.
type ProcessedSnapshot struct {
	Spans []processedSpan
	// Stacks contains stack traces for the goroutines referenced by the Spans
	// through their GoroutineID field.
	Stacks map[int]string // GoroutineID to stack trace
}

const (
	hiddenTagGroupName = "..."
)

var hiddenTags = map[string]struct{}{
	"_unfinished":                {},
	"_verbose":                   {},
	"_dropped_logs":              {},
	"_dropped_children":          {},
	"_dropped_indirect_children": {},
	"node":                       {},
	"store":                      {},
}

type processedSpan struct {
	Operation                     string
	TraceID, SpanID, ParentSpanID uint64
	Start                         time.Time
	GoroutineID                   uint64
	Tags                          []ProcessedTag
	// Current is set if the span is currently present in the active spans
	// registry.
	Current bool
	// CurrentRecordingMode indicates the spans's current recording mode. The
	// field is not set if Current == false.
	CurrentRecordingMode tracingpb.RecordingType
	ChildrenMetadata     map[string]tracingpb.OperationMetadata
}

// ProcessedTag is a span tag that was processed and expanded by processTag.
type ProcessedTag struct {
	Key, Val string
	Caption  string
	Link     string
	Hidden   bool
	// highlight is set if the tag should be rendered with a little exclamation
	// mark.
	Highlight bool

	// inherit is set if this tag should be passed down to children, and
	// recursively.
	Inherit bool
	// inherited is set if this tag was passed over from an ancestor.
	Inherited bool

	PropagateUp bool
	// copiedFromChild is set if this tag did not originate on the owner span, but
	// instead was propagated upwards from a child span.
	CopiedFromChild bool
	Children        []ProcessedChildTag
}

// ProcessedChildTag is a span tag that is embedded as a lazy tag in a parent tag.
type ProcessedChildTag struct {
	Key, Val string
}

func appendRespectingHiddenGroup(tags []ProcessedTag, tag ProcessedTag) []ProcessedTag {
	tags = append(tags, tag)
	endIndex := len(tags) - 1
	if tags[endIndex-1].Key == hiddenTagGroupName {
		tags[endIndex], tags[endIndex-1] = tags[endIndex-1], tags[endIndex]
	}
	return tags
}

// propagateTagUpwards copies tag from sp to all of sp's ancestors.
func propagateTagUpwards(tag ProcessedTag, sp *processedSpan, spans map[uint64]*processedSpan) {
	tag.CopiedFromChild = true
	tag.Inherit = false
	parentID := sp.ParentSpanID
	for {
		p, ok := spans[parentID]
		if !ok {
			return
		}
		p.Tags = appendRespectingHiddenGroup(p.Tags, tag)
		parentID = p.ParentSpanID
	}
}

func propagateInheritTagDownwards(
	tag ProcessedTag, sp *processedSpan, children map[uint64][]*processedSpan,
) {
	tag.PropagateUp = false
	tag.Inherited = true
	tag.Hidden = true
	for _, child := range children[sp.SpanID] {
		child.Tags = appendRespectingHiddenGroup(child.Tags, tag)
		propagateInheritTagDownwards(tag, child, children)
	}
}

// processSpan massages a span for presentation in the UI. Some of the tags are
// expanded.
func processSpan(s tracingpb.RecordedSpan, snap tracing.SpansSnapshot) processedSpan {
	p := processedSpan{
		Operation:        s.Operation,
		TraceID:          uint64(s.TraceID),
		SpanID:           uint64(s.SpanID),
		ParentSpanID:     uint64(s.ParentSpanID),
		Start:            s.StartTime,
		GoroutineID:      s.GoroutineID,
		ChildrenMetadata: s.ChildrenMetadata,
	}

	p.Tags = make([]ProcessedTag, 0)

	// Added to p.Tags if iteration populates the Children.
	hiddenTag := ProcessedTag{
		Key:      hiddenTagGroupName,
		Hidden:   true,
		Children: make([]ProcessedChildTag, 0),
	}

	for _, tagGroup := range s.TagGroups {
		key := tagGroup.Name

		if key == tracingpb.AnonymousTagGroupName {
			// The anonymous tag group.
			// Non-hidden should be treated as top-level.
			// Hidden tags should be moved to the hidden tag group.
			for _, tagKV := range tagGroup.Tags {
				if _, hidden := hiddenTags[tagKV.Key]; hidden {
					hiddenTag.Children = append(hiddenTag.Children, ProcessedChildTag{
						Key: tagKV.Key,
						Val: tagKV.Value,
					})
				} else {
					p.Tags = append(p.Tags, processTag(tagKV.Key, tagKV.Value, snap))
				}
			}
		} else {
			// A named tag group. Each tag should be treated as a child of this parent.
			processedParentTag := ProcessedTag{
				// Don't actually need to call processTag() here, none of the checks
				// will apply to a tag group.
				Key:    key,
				Val:    "",
				Hidden: false,
			}
			processedParentTag.Children = make([]ProcessedChildTag, len(tagGroup.Tags))
			for i, tag := range tagGroup.Tags {
				processedParentTag.Children[i] = ProcessedChildTag{
					Key: tag.Key,
					Val: tag.Value,
				}
			}
			p.Tags = append(p.Tags, processedParentTag)
		}
	}
	if len(hiddenTag.Children) != 0 {
		p.Tags = append(p.Tags, hiddenTag)
	}
	return p
}

// processTag massages span tags for presentation in the UI. It marks some
// tags to be inherited by child spans, and it expands lock contention tags
// with information about the lock holder txn.
func processTag(k, v string, snap tracing.SpansSnapshot) ProcessedTag {
	p := ProcessedTag{
		Key: k,
		Val: v,
	}

	switch k {
	case "lock_holder_txn":
		txnID := v
		// Take only the first 8 bytes, to keep the text shorter.
		txnIDShort := v[:8]
		p.Val = txnIDShort
		p.PropagateUp = true
		p.Highlight = true
		p.Link = txnIDShort
		txnState := findTxnState(txnID, snap)
		if !txnState.found {
			p.Caption = "blocked on unknown transaction"
		} else if txnState.curQuery != "" {
			p.Caption = "blocked on txn currently running query: " + txnState.curQuery
		} else {
			p.Caption = "blocked on idle txn"
		}
	case "statement":
		p.Inherit = true
		p.PropagateUp = true
	}

	return p
}

// txnState represents the current state of a SQL txn, as determined by
// findTxnState. Namely, the state contains the current SQL query running inside
// the transaction, if any.
type txnState struct {
	// found is set if any tracing spans pertaining to this transaction are found.
	found    bool
	curQuery string
}

// findTxnState looks through a snapshot for span pertaining to the specified
// transaction and, within those, looks for a running SQL query.
func findTxnState(txnID string, snap tracing.SpansSnapshot) txnState {
	// Iterate through all the traces and look for a "sql txn" span for the
	// respective transaction.
	for _, t := range snap.Traces {
		for _, s := range t {
			if s.Operation != "sql txn" {
				continue
			}
			txnTagGroup := s.FindTagGroup("txn")
			if txnTagGroup == nil {
				continue
			}
			txnTag, ok := txnTagGroup.FindTag("txn")
			if !ok {
				continue
			}
			if txnTag != txnID {
				continue
			}
			// I've found the transaction. Look through its children and find a SQL query.
			for _, s2 := range t {
				if s2.Operation == "sql query" {
					stmt := ""
					stmtTag := s2.FindTagGroup("statement")
					if stmtTag != nil {
						stmt, _ = stmtTag.FindTag("statement")
					}
					return txnState{
						found:    true,
						curQuery: stmt,
					}
				}
			}
			return txnState{found: true}
		}
	}
	return txnState{found: false}
}
