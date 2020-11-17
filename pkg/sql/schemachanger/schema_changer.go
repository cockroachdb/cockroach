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

type runDependencies interface {
	withDescriptorMutationDeps(context.Context, func(ctx2 context.Context, deps depsForDescriptorMutation) error) error
}

func (sc *SchemaChanger) Run(ctx context.Context, deps runDependencies) error {
	steps, err := compileStateToForwardSteps(ctx, sc.state)
	if err != nil {
		return err
	}
	for _, step := range steps {
		switch t := step.(type) {
		case descriptorMutationStep:
			if err := deps.withDescriptorMutationDeps(ctx, t.run); err != nil {
				return err
			}
		}
		step.apply(ctx, sc)
	}
	return nil
}
