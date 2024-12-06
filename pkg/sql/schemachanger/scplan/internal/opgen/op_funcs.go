// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

func statementForDropJob(e scpb.Element, md *opGenContext) scop.StatementForDropJob {
	stmtID := md.Targets[md.elementToTarget[e]].Metadata.StatementID
	stmt := md.Statements[stmtID].RedactedStatement.StripMarkers()
	switch e.(type) {
	case *scpb.PrimaryIndex:
		stmt = "removed primary index; " + stmt
	case *scpb.SecondaryIndex:
		stmt = "removed secondary index; " + stmt
	case *scpb.TemporaryIndex:
		stmt = "removed temporary index; " + stmt
	}
	return scop.StatementForDropJob{
		// Using the redactable string but with stripped markers gives us a
		// normalized and fully-qualified string value for display use.
		Statement:   stmt,
		StatementID: stmtID,
		Rollback:    md.InRollback,
	}
}

// opGenContext is one of the available arguments to an opgen
// function. It allows access to the fields of the TargetState and, via
// a lookup map, the fields of the element itself.
//
// This map allows opgen functions to find their target without an O(N)
// lookup.
type opGenContext struct {
	scpb.TargetState
	Current         []scpb.Status
	Initial         []scpb.Status
	ActiveVersion   clusterversion.ClusterVersion
	elementToTarget map[scpb.Element]int
	InRollback      bool
}

func makeOpgenContext(
	activeVersion clusterversion.ClusterVersion, cs scpb.CurrentState,
) opGenContext {
	md := opGenContext{
		ActiveVersion:   activeVersion,
		InRollback:      cs.InRollback,
		TargetState:     cs.TargetState,
		Initial:         cs.Initial,
		Current:         cs.Current,
		elementToTarget: make(map[scpb.Element]int),
	}
	for i := range cs.Targets {
		e := cs.Targets[i].Element()
		if prev, exists := md.elementToTarget[e]; exists {
			panic(errors.AssertionFailedf(
				"duplicate targets for %s: %v and %v", screl.ElementString(e),
				cs.Targets[i].TargetStatus, cs.Targets[prev].TargetStatus,
			))
		}
		md.elementToTarget[e] = i
	}
	return md
}

// opsFunc are a fully-compiled and checked set of functions to emit operations
// given an element value.
type opsFunc func(element scpb.Element, md *opGenContext) []scop.Op

func makeOpsFunc(el scpb.Element, fns []interface{}) (opsFunc, scop.Type, error) {
	var opType scop.Type
	var funcValues []reflect.Value
	for _, fn := range fns {
		typ, err := checkOpFunc(el, fn)
		if err != nil {
			return nil, 0, err
		}
		if len(funcValues) > 0 && typ != opType {
			return nil, 0, errors.Errorf("conflicting operation types for %T: %s != %s",
				el, opType, typ)
		}
		opType = typ
		funcValues = append(funcValues, reflect.ValueOf(fn))
	}
	return func(element scpb.Element, md *opGenContext) []scop.Op {
		ret := make([]scop.Op, 0, len(funcValues))
		in := []reflect.Value{reflect.ValueOf(element)}
		inWithMeta := []reflect.Value{reflect.ValueOf(element), reflect.ValueOf(md)}
		for _, fn := range funcValues {
			var out []reflect.Value
			if fn.Type().NumIn() == 1 {
				out = fn.Call(in)
			} else {
				out = fn.Call(inWithMeta)
			}
			if !out[0].IsNil() {
				ret = append(ret, out[0].Interface().(scop.Op))
			}
		}
		return ret
	}, opType, nil
}

var (
	opInterfaceType                  = reflect.TypeOf((*scop.Op)(nil)).Elem()
	immediateMutationOpInterfaceType = reflect.TypeOf((*scop.ImmediateMutationOp)(nil)).Elem()
	deferredMutationOpInterfaceType  = reflect.TypeOf((*scop.DeferredMutationOp)(nil)).Elem()
	validationOpInterfaceType        = reflect.TypeOf((*scop.ValidationOp)(nil)).Elem()
	backfillOpInterfaceType          = reflect.TypeOf((*scop.BackfillOp)(nil)).Elem()
)

func checkOpFunc(el scpb.Element, fn interface{}) (opType scop.Type, _ error) {
	fnV := reflect.ValueOf(fn)
	fnT := fnV.Type()
	if fnT.Kind() != reflect.Func {
		return 0, errors.Errorf(
			"%v is a %s, expected %s", fnT, fnT.Kind(), reflect.Func,
		)
	}
	elType := reflect.TypeOf(el)
	if !(fnT.NumIn() == 1 && fnT.In(0) == elType) &&
		!(fnT.NumIn() == 2 && fnT.In(0) == elType &&
			fnT.In(1) == reflect.TypeOf((*opGenContext)(nil))) {
		return 0, errors.Errorf(
			"expected %v to be a func with one argument of type %s", fnT, elType,
		)
	}
	returnTypeError := func() error {
		return errors.Errorf(
			"expected %v to be a func with one return value of a "+
				"pointer type which implements %s", fnT, opType,
		)
	}
	if fnT.NumOut() != 1 {
		return 0, returnTypeError()
	}
	out := fnT.Out(0)
	if out.Kind() != reflect.Ptr || !out.Implements(opInterfaceType) {
		return 0, returnTypeError()
	}
	switch {
	case out.Implements(immediateMutationOpInterfaceType), out.Implements(deferredMutationOpInterfaceType):
		opType = scop.MutationType
	case out.Implements(validationOpInterfaceType):
		opType = scop.ValidationType
	case out.Implements(backfillOpInterfaceType):
		opType = scop.BackfillType
	default:
		return 0, errors.AssertionFailedf("%s implemented %s but does not conform to any known type",
			out, opInterfaceType)
	}
	return opType, nil
}

// checkIfDescriptorIsWithoutData given the context this will determine if
// a descriptor is an added state, and has no data. This can allow us to
// skip certain operations like backfills / validation.
func checkIfDescriptorIsWithoutData(id descpb.ID, md *opGenContext) bool {
	doesDescriptorHaveData := false
	for idx, t := range md.Targets {
		// Validate this is the descriptor ID we are
		// looking for.
		if screl.GetDescID(t.Element()) != id {
			continue
		}
		switch t.Element().(type) {
		case *scpb.IndexData, *scpb.TableData:
			// Check if this descriptor has any data within
			// a public state.
			if md.Current[idx] == scpb.Status_PUBLIC &&
				md.Initial[idx] == scpb.Status_PUBLIC {
				doesDescriptorHaveData = true
			}
		}
		if doesDescriptorHaveData {
			break
		}
	}
	return !doesDescriptorHaveData
}

// checkIfZoneConfigHasGCDependents will determine if a table/database
// descriptor has data dependencies it still needs to GC. This allows us to
// determine when we need to skip certain operations like deleting a zone
// config.
func checkIfZoneConfigHasGCDependents(elem scpb.ZoneConfigElement, md *opGenContext) bool {
	isValidTableData := func(td *scpb.TableData) bool {
		switch e := elem.(type) {
		case *scpb.DatabaseZoneConfig:
			return e.DatabaseID == td.DatabaseID
		case *scpb.TableZoneConfig:
			return e.TableID == td.TableID
		default:
			panic(errors.AssertionFailedf(
				"element type %T not allowed for checkIfZoneConfigHasGCDependents", e))
		}
	}
	for idx, t := range md.Targets {
		switch e := t.Element().(type) {
		case *scpb.TableData:
			// Filter out any elements we do not want to consider.
			if !isValidTableData(e) {
				continue
			}
			// Check if this descriptor has any data marked for an absent state.
			if t.TargetStatus == scpb.Status_ABSENT &&
				md.Initial[idx] == scpb.Status_PUBLIC {
				return true
			}
		}
	}
	return false
}

// checkIfIndexHasGCDependents is like checkIfZoneConfigHasGCDependents, but
// for indexes. We also ensure that we filter out irrelevant indexes here.
func checkIfIndexHasGCDependents(tableID descpb.ID, md *opGenContext) bool {
	for idx, t := range md.Targets {
		switch e := t.Element().(type) {
		case *scpb.IndexData:
			// Validate this is the table ID we are
			// looking for.
			if e.TableID != tableID {
				continue
			}
			// Check if this descriptor has any data marked for an absent state.
			if t.TargetStatus == scpb.Status_ABSENT &&
				md.Initial[idx] == scpb.Status_PUBLIC {
				return true
			}
		}
	}
	return false
}
