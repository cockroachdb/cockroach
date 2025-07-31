// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scplan

import (
	gojson "encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scstage"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"gopkg.in/yaml.v2"
)

// DecorateErrorWithPlanDetails adds plan graphviz URLs as error details.
func (p Plan) DecorateErrorWithPlanDetails(err error) (retErr error) {
	if err == nil {
		return nil
	}
	if len(p.Stages) > 0 {
		err = addDetail(err, "EXPLAIN plan", "", p.ExplainVerbose)
		err = addDetail(err, "EXPLAIN graphviz URL", "stages graphviz: ", p.StagesURL)
	}
	if p.Graph != nil {
		err = addDetail(err, "dependencies graphviz URL", "dependencies graphviz: ", p.DependenciesURL)
	}
	return err
}

func addDetail(
	undecoratedError error, errWrapStr, detailFmtStr string, genDetail func() (string, error),
) (err error) {
	err = undecoratedError
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("%v", r)
			}
			rAsErr = errors.Wrapf(rAsErr, "panic when generating plan error detail %s", errWrapStr)
			err = errors.CombineErrors(err, rAsErr)
		}
	}()
	detail, detailErr := genDetail()
	if detailErr == nil {
		return errors.WithDetailf(err, detailFmtStr+"%s", detail)
	}
	detailErr = errors.Wrapf(detailErr, "error when generating plan error detail %s", errWrapStr)
	return errors.CombineErrors(err, detailErr)
}

// DependenciesURL returns a URL to render the dependency graph in the Plan.
func (p Plan) DependenciesURL() (string, error) {
	return scgraphviz.DependenciesURL(p.CurrentState, p.Graph)
}

// StagesURL returns a URL to render the stages in the Plan.
func (p Plan) StagesURL() (string, error) {
	return scgraphviz.StagesURL(p.CurrentState, p.Graph, p.Stages)
}

// ExplainViz returns graphviz renderings for EXPLAIN (DDL, VIZ) statements.
func (p Plan) ExplainViz() (stagesURL, depsURL string, err error) {
	stagesURL, err = p.StagesURL()
	if err != nil {
		return "", "", err
	}
	depsURL, err = p.DependenciesURL()
	if err != nil {
		return "", "", err
	}
	return stagesURL, depsURL, nil
}

// ExplainCompact returns a human-readable plan rendering for
// EXPLAIN (DDL) statements.
func (p Plan) ExplainCompact() (string, error) {
	return p.explain(treeprinter.DefaultStyle)
}

// ExplainVerbose returns a human-readable plan rendering for
// EXPLAIN (DDL, VERBOSE) statements.
func (p Plan) ExplainVerbose() (string, error) {
	return p.explain(treeprinter.BulletStyle)
}

func (p Plan) explain(style treeprinter.Style) (string, error) {
	tp := treeprinter.NewWithStyle(style)
	root := tp.Child(p.rootNodeLabel())
	var pn treeprinter.Node
	for i, s := range p.Stages {
		// Generate stage node, grouped by phase.
		if i == 0 || s.Phase != p.Stages[i-1].Phase {
			pn = root.Childf("%s", s.Phase)
		}
		sn := pn.Childf("Stage %d of %d in %s", s.Ordinal, s.StagesInPhase, s.Phase)
		// Generate status transition nodes, grouped by target type.
		if err := p.explainTargets(s, sn, style); err != nil {
			return "", err
		}
		// Generate operations nodes.
		if err := p.explainOps(s, sn, style); err != nil {
			return "", err
		}
	}
	return tp.String(), nil
}

func (p Plan) explainTargets(s scstage.Stage, sn treeprinter.Node, style treeprinter.Style) error {
	var targetTypeMap util.FastIntMap
	depEdgeByElement := make(map[scpb.Element][]*scgraph.DepEdge)
	var beforeMaxLen, afterMaxLen int
	// Collect non-empty target status groupings for this stage.
	for j, before := range s.Before {
		t := &p.TargetState.Targets[j]
		after := s.After[j]
		if before == after {
			continue
		}
		// Update status string max lengths for width-aligned formatting.
		if l := utf8.RuneCountInString(before.String()); l > beforeMaxLen {
			beforeMaxLen = l
		}
		if l := utf8.RuneCountInString(after.String()); l > afterMaxLen {
			afterMaxLen = l
		}
		// Update grouping size.
		k := int(scpb.AsTargetStatus(t.TargetStatus))
		numTransitions, found := targetTypeMap.Get(k)
		if !found {
			targetTypeMap.Set(k, 1)
		} else {
			targetTypeMap.Set(k, numTransitions+1)
		}
		// Collect rules affecting this element's status transitions.
		if style == treeprinter.BulletStyle && !s.IsResetPreCommitStage() {
			n, nodeFound := p.Graph.GetNode(t, before)
			if !nodeFound {
				return errors.Errorf("could not find node [[%s, %s], %s] in graph",
					screl.ElementString(t.Element()), t.TargetStatus, before)
			}
			for n.CurrentStatus != after {
				oe, edgeFound := p.Graph.GetOpEdgeFrom(n)
				if !edgeFound {
					return errors.Errorf("could not find op edge from %s in graph", screl.NodeString(n))
				}
				n = oe.To()
				if err := p.Graph.ForEachDepEdgeTo(n, func(de *scgraph.DepEdge) error {
					depEdgeByElement[t.Element()] = append(depEdgeByElement[t.Element()], de)
					return nil
				}); err != nil {
					return err
				}
			}
		}
	}
	// Generate format string for printing element status transition.
	fmtCompactTransition := fmt.Sprintf("%%-%ds → %%-%ds %%s", beforeMaxLen, afterMaxLen)
	// Go over each target grouping.
	for _, ts := range []scpb.TargetStatus{scpb.ToPublic, scpb.Transient, scpb.ToAbsent} {
		numTransitions := targetTypeMap.GetDefault(int(ts))
		if numTransitions == 0 {
			continue
		}
		plural := "s"
		if numTransitions == 1 {
			plural = ""
		}
		tn := sn.Childf("%d element%s transitioning toward %s",
			numTransitions, plural, ts.Status())
		for j, before := range s.Before {
			t := &p.TargetState.Targets[j]
			after := s.After[j]
			if t.TargetStatus != ts.Status() || before == after {
				continue
			}
			// Add element node and child rule nodes, and monitor memory allocation.
			var en treeprinter.Node
			var estimatedMemAlloc int
			accountFor := func(label string) string {
				estimatedMemAlloc += len(label)
				return label
			}
			var sb redact.StringBuilder
			if err := screl.FormatTargetElement(&sb, p.TargetState, t.Element()); err != nil {
				return err
			}
			if style == treeprinter.BulletStyle {
				en = tn.Child(accountFor(sb.String()))
				en.AddLine(accountFor(fmt.Sprintf("%s → %s", before, after)))
			} else {
				en = tn.Child(accountFor(fmt.Sprintf(fmtCompactTransition, before, after, sb.String())))
			}
			depEdges := depEdgeByElement[t.Element()]
			for _, de := range depEdges {
				var sbf redact.StringBuilder
				if err := screl.FormatTargetElement(&sbf, p.TargetState, de.From().Element()); err != nil {
					return err
				}
				rn := en.Child(accountFor(fmt.Sprintf("%s dependency from %s %s",
					de.Kind(), de.From().CurrentStatus, sbf)))
				for _, r := range de.Rules() {
					rn.AddLine(accountFor(fmt.Sprintf("rule: %q", r.Name)))
				}
			}
			if err := p.Params.MemAcc.Grow(p.Params.Ctx, int64(estimatedMemAlloc)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p Plan) explainOps(s scstage.Stage, sn treeprinter.Node, style treeprinter.Style) error {
	ops := s.Ops()
	if len(ops) == 0 {
		return nil
	}
	plural := "s"
	if len(ops) == 1 {
		plural = ""
	}
	on := sn.Childf("%d %s operation%s", len(ops), strings.TrimSuffix(s.Type().String(), "Type"), plural)
	for _, op := range ops {
		var estimatedMemAlloc int
		accountFor := func(label string) string {
			estimatedMemAlloc += len(label)
			return label
		}
		if setJobStateOp, ok := op.(*scop.SetJobStateOnDescriptor); ok {
			clone := *setJobStateOp
			clone.State = scpb.DescriptorState{}
			op = &clone
		}
		name := opName(op)
		if style == treeprinter.BulletStyle {
			n := on.Child(accountFor(name))
			opBody, err := explainOpBodyVerbose(op)
			if err != nil {
				return err
			}
			for _, line := range strings.Split(opBody, "\n") {
				n.AddLine(accountFor(line))
			}
		} else {
			opBody, err := explainOpBodyCompact(op)
			if err != nil {
				return err
			}
			if len(opBody) == 0 {
				on.Child(accountFor(name))
			} else {
				on.Child(accountFor(fmt.Sprintf("%s %s", name, opBody)))
			}
		}
		if err := p.Params.MemAcc.Grow(p.Params.Ctx, int64(estimatedMemAlloc)); err != nil {
			return err
		}
	}
	return nil
}

func explainOpBodyVerbose(op scop.Op) (string, error) {
	opMap, err := scgraphviz.ToMap(op)
	if err != nil {
		return "", err
	}
	yml, err := yaml.Marshal(opMap)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(yml), "\n"), nil
}

func explainOpBodyCompact(op scop.Op) (string, error) {
	m := map[string]interface{}{}
	// Round-trip the op through the json marshaller to make it easier to use
	// reflection afterwards.
	{
		opMap, err := scgraphviz.ToMap(op)
		if err != nil {
			return "", err
		}
		jb, err := gojson.Marshal(opMap)
		if err != nil {
			return "", err
		}
		if err := gojson.Unmarshal(jb, &m); err != nil {
			return "", err
		}
	}
	// Trim the map to get rid of long strings, nested lists and objects, etc.
	tm := opMapTrim(m)
	if len(tm) == 0 {
		// We've been trimming too aggressively.
		// Add nested objects back, but trim them first.
		for k, v := range m {
			vm, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			tvm := opMapTrim(vm)
			if len(tvm) == 0 {
				continue
			}
			tm[k] = tvm
		}
		if len(tm) == 0 {
			return "", nil
		}
	}
	jb, err := gojson.Marshal(tm)
	return string(jb), err
}

func opMapTrim(in map[string]interface{}) map[string]interface{} {
	const jobIDKey = "JobID"
	out := make(map[string]interface{})
	for k, v := range in {
		vv := reflect.ValueOf(v)
		switch vv.Type().Kind() {
		case reflect.Bool:
			out[k] = vv.Bool()
		case reflect.Float64:
			if k != jobIDKey || vv.Float() != 1 {
				out[k] = vv.Float()
			}
		case reflect.String:
			if str := vv.String(); utf8.RuneCountInString(str) > 16 {
				out[k] = fmt.Sprintf("%.16s...", str)
			} else {
				out[k] = str
			}
		}
	}
	return out
}

// ExplainShape returns a human-readable plan rendering for
// EXPLAIN (DDL, SHAPE) statements.
// It gives a condensed summary of the operations involved in the schema change
// with highlights on potentially expensive operations like backfilling indexes
// and validating constraints. This helps users get a sense of the performance
// impact of executing the schema change.
func (p Plan) ExplainShape() (string, error) {
	// Generate root node.
	tp := treeprinter.NewWithStyle(treeprinter.DefaultStyle)
	root := tp.Child(p.rootNodeLabel())
	var txnCounter uint
	for i, s := range p.Stages {
		switch s.Type() {
		case scop.MutationType:
			// Group these into consecutive transactions.
			// Each transaction is assumed to have a fixed cost, which is true
			// enough in comparison to non-mutation stages whose ops cost O(rows).
			var nextStage *Stage
			if i < len(p.Stages)-1 {
				nextStage = &p.Stages[i+1]
			}
			if nextStage != nil && nextStage.Phase <= scop.PreCommitPhase {
				// Ignore all stages prior to the last pre-commit stage
				// when it comes to counting transactions.
				continue
			}
			txnCounter++
			if nextStage == nil || nextStage.Type() != scop.MutationType {
				// This stage is the last consecutive mutation stage.
				if txnCounter == 1 {
					root.Childf("execute 1 system table mutations transaction")
				} else {
					root.Childf("execute %d system table mutations transactions", txnCounter)
				}
				txnCounter = 0
			}
		case scop.BackfillType:
			if err := p.explainBackfillsAndMerges(root, s.EdgeOps); err != nil {
				return "", err
			}
		case scop.ValidationType:
			if err := p.explainValidations(root, s.EdgeOps); err != nil {
				return "", err
			}
		default:
			return "", errors.AssertionFailedf("unsupported type %s", s.Type())
		}
	}
	return tp.String(), nil
}

func (p Plan) rootNodeLabel() string {
	var sb strings.Builder
	// Don't bother to memory monitor this "prologue" part as it is small and fixed.
	sb.WriteString("Schema change plan for ")
	if p.InRollback {
		sb.WriteString("rolling back ")
	}
	lastStmt := p.Statements[len(p.Statements)-1].RedactedStatement
	sb.WriteString(strings.TrimSuffix(string(lastStmt), ";"))
	if len(p.Statements) > 1 {
		sb.WriteString("; following ")
		for i, stmt := range p.Statements[:len(p.Statements)-1] {
			if i > 0 {
				sb.WriteString("; ")
			}
			sb.WriteString(strings.TrimSuffix(string(stmt.RedactedStatement), ";"))
		}
	}
	sb.WriteString(";")
	return sb.String()
}

func (p Plan) explainBackfillsAndMerges(root treeprinter.Node, ops []scop.Op) error {
	gbs, gms := groupBackfillsAndMerges(ops)
	var estimatedMemAlloc int
	accountFor := func(label string) string {
		estimatedMemAlloc += len(label)
		return label
	}
	for _, gb := range gbs {
		bn := root.Child(accountFor(fmt.Sprintf(
			"backfill using primary index %s in relation %s",
			p.IndexName(gb.relationID, gb.srcIndexID),
			p.Name(gb.relationID),
		)))
		var key, suffix, stored []*scpb.IndexColumn
		for _, t := range p.Targets {
			switch e := t.Element().(type) {
			case *scpb.IndexColumn:
				if e.TableID == gb.relationID {
					switch e.Kind {
					case scpb.IndexColumn_KEY:
						key = append(key, e)
					case scpb.IndexColumn_KEY_SUFFIX:
						suffix = append(suffix, e)
					case scpb.IndexColumn_STORED:
						stored = append(stored, e)
					}
				}
			}
		}
		for _, ics := range [][]*scpb.IndexColumn{key, suffix, stored} {
			sort.Slice(ics, func(i, j int) bool {
				if ics[i].IndexID == ics[j].IndexID {
					return ics[i].OrdinalInKind == ics[j].OrdinalInKind
				}
				return ics[i].IndexID < ics[j].IndexID
			})
		}
		for _, id := range gb.dstIndexIDs.Ordered() {
			var sb strings.Builder
			sb.WriteString("into ")
			sb.WriteString(p.IndexName(gb.relationID, id))
			sb.WriteString(" (")
			for i, ics := range [][]*scpb.IndexColumn{key, suffix, stored} {
				var notFirstColumnInKind bool
				for _, ic := range ics {
					if ic.IndexID != id {
						continue
					}
					if notFirstColumnInKind {
						// Separator between columns of same kind.
						sb.WriteString(", ")
					} else if i == 1 {
						// Separator between key column and key suffix column.
						sb.WriteString(": ")
					} else if i == 2 {
						// Separator between key or key suffix column and stored column.
						sb.WriteString("; ")
					}
					sb.WriteString(p.ColumnName(gb.relationID, ic.ColumnID))
					notFirstColumnInKind = true
				}
			}
			sb.WriteString(")")
			bn.Child(accountFor(sb.String()))
		}
	}
	for _, gm := range gms {
		mn := root.Child(accountFor(fmt.Sprintf(
			"merge temporary indexes into backfilled indexes in relation %s",
			p.Name(gm.relationID),
		)))
		for _, mp := range gm.pairs {
			mn.Child(accountFor(fmt.Sprintf(
				"from %s into %s",
				p.IndexName(gm.relationID, mp.srcIndexID),
				p.IndexName(gm.relationID, mp.dstIndexID),
			)))
		}
	}
	return p.Params.MemAcc.Grow(p.Params.Ctx, int64(estimatedMemAlloc))
}

func (p Plan) explainValidations(root treeprinter.Node, ops []scop.Op) error {
	var estimatedMemAlloc int
	accountFor := func(label string) string {
		estimatedMemAlloc += len(label)
		return label
	}
	for _, op := range ops {
		switch op := op.(type) {
		case *scop.ValidateIndex:
			root.Child(accountFor(fmt.Sprintf(
				"validate UNIQUE constraint backed by index %s in relation %s",
				p.IndexName(op.TableID, op.IndexID),
				p.Name(op.TableID),
			)))
		case *scop.ValidateConstraint:
			root.Child(accountFor(fmt.Sprintf(
				"validate non-index-backed constraint %s in relation %s",
				p.ConstraintName(op.TableID, op.ConstraintID),
				p.Name(op.TableID),
			)))
		case *scop.ValidateColumnNotNull:
			root.Child(accountFor(fmt.Sprintf(
				"validate NOT NULL constraint on column %s in index %s in relation %s",
				p.ColumnName(op.TableID, op.ColumnID),
				p.IndexName(op.TableID, op.IndexIDForValidation),
				p.Name(op.TableID),
			)))
		}
	}
	return p.Params.MemAcc.Grow(p.Params.Ctx, int64(estimatedMemAlloc))
}

func groupBackfillsAndMerges(ops []scop.Op) ([]groupedBackfill, []groupedMerge) {
	gbs := make([]groupedBackfill, 0, len(ops))
	gms := make([]groupedMerge, 0, len(ops))
	gbIdx := make(map[groupedBackfillKey]int)
	gmIdx := make(map[groupedMergeKey]int)
	for _, op := range ops {
		switch op := op.(type) {
		case *scop.BackfillIndex:
			k := groupedBackfillKey{
				relationID: op.TableID,
				srcIndexID: op.SourceIndexID,
			}
			if idx, ok := gbIdx[k]; ok {
				gbs[idx].dstIndexIDs.Add(op.IndexID)
			} else {
				gbIdx[k] = len(gbs)
				gbs = append(gbs, groupedBackfill{
					groupedBackfillKey: k,
					dstIndexIDs:        catid.MakeIndexIDSet(op.IndexID),
				})
			}
		case *scop.MergeIndex:
			k := groupedMergeKey{relationID: op.TableID}
			mp := mergePair{
				srcIndexID: op.TemporaryIndexID,
				dstIndexID: op.BackfilledIndexID,
			}
			if idx, ok := gmIdx[k]; ok {
				gms[idx].pairs = append(gms[idx].pairs, mp)
			} else {
				gmIdx[k] = len(gms)
				gms = append(gms, groupedMerge{
					groupedMergeKey: k,
					pairs:           []mergePair{mp},
				})
			}
		}
	}
	sort.Slice(gbs, func(i, j int) bool {
		if gbs[i].relationID == gbs[j].relationID {
			return gbs[i].srcIndexID < gbs[j].srcIndexID
		}
		return gbs[i].relationID < gbs[j].relationID
	})
	sort.Slice(gms, func(i, j int) bool {
		return gms[i].relationID < gms[j].relationID
	})
	for _, gm := range gms {
		sort.Slice(gm.pairs, func(i, j int) bool {
			return gm.pairs[i].srcIndexID < gm.pairs[j].dstIndexID
		})
	}
	return gbs, gms
}

type groupedBackfill struct {
	groupedBackfillKey
	dstIndexIDs catid.IndexSet
}

type groupedBackfillKey struct {
	relationID catid.DescID
	srcIndexID catid.IndexID
}

type groupedMerge struct {
	groupedMergeKey
	pairs []mergePair
}

type groupedMergeKey struct {
	relationID catid.DescID
}

type mergePair struct {
	srcIndexID, dstIndexID catid.IndexID
}

func opName(op scop.Op) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", op), "*scop.")
}
