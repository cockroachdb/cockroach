// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	gojson "encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scstage"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
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
	// Generate root node.
	tp := treeprinter.NewWithStyle(style)
	var sb strings.Builder
	{
		// Don't bother to memory monitor this "prologue" part as it is small and fixed.
		sb.WriteString("Schema change plan for ")
		if p.InRollback {
			sb.WriteString("rolling back ")
		}
		lastStmt := p.Statements[len(p.Statements)-1].RedactedStatement
		sb.WriteString(strings.TrimSuffix(lastStmt, ";"))
		if len(p.Statements) > 1 {
			sb.WriteString("; following ")
			for i, stmt := range p.Statements[:len(p.Statements)-1] {
				if i > 0 {
					sb.WriteString("; ")
				}
				sb.WriteString(strings.TrimSuffix(stmt.RedactedStatement, ";"))
			}
		}
		sb.WriteString(";")
	}

	root := tp.Child(sb.String())
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
	noOpByElement := make(map[scpb.Element][]*scgraph.OpEdge)
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
				if p.Graph.IsNoOp(oe) {
					noOpByElement[t.Element()] = append(noOpByElement[t.Element()], oe)
				}
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
			var str string
			var estimatedMemAlloc int
			if style == treeprinter.BulletStyle {
				str = screl.ElementString(t.Element())
				en = tn.Child(str)
				estimatedMemAlloc += len(str)
				str = fmt.Sprintf("%s → %s", before, after)
				en.AddLine(str)
				estimatedMemAlloc += len(str)
			} else {
				str = fmt.Sprintf(fmtCompactTransition, before, after, screl.ElementString(t.Element()))
				en = tn.Child(str)
				estimatedMemAlloc += len(str)
			}
			depEdges := depEdgeByElement[t.Element()]
			for _, de := range depEdges {
				str = fmt.Sprintf("%s dependency from %s %s",
					de.Kind(), de.From().CurrentStatus, screl.ElementString(de.From().Element()))
				rn := en.Child(str)
				estimatedMemAlloc += len(str)
				for _, r := range de.Rules() {
					str = fmt.Sprintf("rule: %q", r.Name)
					rn.AddLine(str)
					estimatedMemAlloc += len(str)
				}
			}
			noOpEdges := noOpByElement[t.Element()]
			for _, oe := range noOpEdges {
				noOpRules := p.Graph.NoOpRules(oe)
				if len(noOpRules) == 0 {
					continue
				}
				str = fmt.Sprintf("skip %s → %s operations",
					oe.From().CurrentStatus, oe.To().CurrentStatus)
				nn := en.Child(str)
				estimatedMemAlloc += len(str)
				for _, rule := range noOpRules {
					str = fmt.Sprintf("rule: %q", rule)
					nn.AddLine(str)
					estimatedMemAlloc += len(str)
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
		if setJobStateOp, ok := op.(*scop.SetJobStateOnDescriptor); ok {
			clone := *setJobStateOp
			clone.State = scpb.DescriptorState{}
			op = &clone
		}
		opName := strings.TrimPrefix(fmt.Sprintf("%T", op), "*scop.")
		if style == treeprinter.BulletStyle {
			n := on.Child(opName)
			estimatedMemAlloc += len(opName)
			opBody, err := explainOpBodyVerbose(op)
			if err != nil {
				return err
			}
			for _, line := range strings.Split(opBody, "\n") {
				n.AddLine(line)
				estimatedMemAlloc += len(line)
			}
		} else {
			opBody, err := explainOpBodyCompact(op)
			if err != nil {
				return err
			}
			if len(opBody) == 0 {
				on.Child(opName)
				estimatedMemAlloc += len(opName)
			} else {
				str := fmt.Sprintf("%s %s", opName, opBody)
				on.Child(str)
				estimatedMemAlloc += len(str)
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
