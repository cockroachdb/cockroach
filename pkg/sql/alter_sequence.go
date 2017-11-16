package sql

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type alterSequenceNode struct {
	n       *tree.AlterSequence
	seqDesc *sqlbase.TableDescriptor
}

func (p *planner) AlterSequence(ctx context.Context, n *tree.AlterSequence) (planNode, error) {
	tn, err := n.Name.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	seqDesc, err := getSequenceDesc(ctx, p.txn, p.getVirtualTabler(), tn)
	if err != nil {
		return nil, err
	}
	if seqDesc == nil {
		if n.IfExists {
			return &zeroNode{}, nil
		}
		return nil, sqlbase.NewUndefinedRelationError(tn)
	}

	if err := p.CheckPrivilege(seqDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &alterSequenceNode{n: n, seqDesc: seqDesc}, nil
}

func (n *alterSequenceNode) Start(params runParams) error {
	desc := n.seqDesc

	err := assignSequenceOptions(desc.SequenceOpts, n.n.Options, false /* setDefaults */)
	if err != nil {
		return err
	}

	if err := params.p.writeTableDesc(params.ctx, n.seqDesc); err != nil {
		return err
	}

	// Record this sequence alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(params.p.LeaseMgr()).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogAlterSequence,
		int32(n.seqDesc.ID),
		int32(params.p.evalCtx.NodeID),
		struct {
			SequenceName string
			Statement    string
			User         string
		}{n.seqDesc.Name, n.n.String(), params.p.session.User},
	); err != nil {
		return err
	}

	params.p.notifySchemaChange(n.seqDesc, sqlbase.InvalidMutationID)

	return nil
}

func (n *alterSequenceNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterSequenceNode) Close(context.Context)        {}
func (n *alterSequenceNode) Values() tree.Datums          { return tree.Datums{} }

// assignSequenceOptions moves options from the AST node to the sequence options descriptor.
func assignSequenceOptions(
	optsDesc *sqlbase.TableDescriptor_SequenceOpts, optsNode tree.SequenceOptions, setDefaults bool,
) error {
	// All other defaults are dependent on the value of increment,
	// i.e. whether the sequence is ascending or descending.
	for _, option := range optsNode {
		if option.Name == tree.SeqOptIncrement {
			optsDesc.Increment = *option.IntVal
		}
	}
	if optsDesc.Increment == 0 {
		return pgerror.NewError(
			pgerror.CodeInvalidParameterValueError, "INCREMENT must not be zero")
	}
	isAscending := optsDesc.Increment > 0

	// Set increment-dependent defaults.
	if setDefaults {
		if isAscending {
			optsDesc.MinValue = 1
			optsDesc.MaxValue = math.MaxInt64
			optsDesc.Start = optsDesc.MinValue
		} else {
			optsDesc.MinValue = math.MinInt64
			optsDesc.MaxValue = -1
			optsDesc.Start = optsDesc.MaxValue
		}
	}

	// Fill in all other options.
	optionsSeen := map[string]bool{}
	for _, option := range optsNode {
		// Error on duplicate options.
		_, seenBefore := optionsSeen[option.Name]
		if seenBefore {
			return pgerror.NewError(pgerror.CodeSyntaxError, "conflicting or redundant options")
		}
		optionsSeen[option.Name] = true

		switch option.Name {
		case tree.SeqOptIncrement:
			// Do nothing; this has already been set.
		case tree.SeqOptMinValue:
			// A value of nil represents the user explicitly saying `NO MINVALUE`.
			if option.IntVal != nil {
				optsDesc.MinValue = *option.IntVal
			}
		case tree.SeqOptMaxValue:
			// A value of nil represents the user explicitly saying `NO MAXVALUE`.
			if option.IntVal != nil {
				optsDesc.MaxValue = *option.IntVal
			}
		case tree.SeqOptStart:
			optsDesc.Start = *option.IntVal
		case tree.SeqOptCycle:
			optsDesc.Cycle = option.BoolVal
		}
	}

	return nil
}
