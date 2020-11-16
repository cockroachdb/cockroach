package schemachanger

import "context"

// SchemaChanger coordinates schema changes.
//
// It exists in the following contexts:
//
//  1) Exeuction of a transaction which issues DDLs
//  2) Schema change jobs
//  3) ? Type change jobs ?
//
type SchemaChanger struct {
	state schemaChangerState
}

// TODO list:
//
// - Define op interface
// - Define a basic library of ops needed for our prototype
// - Implement those ops
// - Define set of elements needed
// - Compile elements to success path (defer failure path)
// - Deal with serialization and deserialization of schema changer state
//   into jobs and table descriptor
// - Plumb it together

/*
type op interface{}

var ops = []step{
	{
		addColumnChangeStateOp{tableID, columnID, nextState},
		addIndexChanageState{tableID, indexID, nextState},
	},
	{
		paralelBackfill{
			backfill{foo},
			backfill{bar},
		},
  },
		addIndexChangeState{foo, Backfilled},
		addIndexChangeState{bar, Backfilled},
	},
}
*/

// TODO: Implement
func (sc *SchemaChanger) Step(ctx context.Context) error {
	panic("not implemented")
}

// func (sc *SchemaChanger) alterTableCmd()
