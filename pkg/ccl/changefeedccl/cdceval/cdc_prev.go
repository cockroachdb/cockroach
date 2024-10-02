// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdceval

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type prevCol struct {
	name tree.Name
	t    *types.T
	d    *descpb.ColumnDescriptor
	id   descpb.ColumnID
}

var _ catalog.Column = (*prevCol)(nil)

func newPrevColumnForDesc(desc *cdcevent.EventDescriptor) (catalog.Column, error) {
	colExists := func(n tree.Name) bool {
		return catalog.FindColumnByTreeName(desc.TableDescriptor(), n) != nil
	}

	prevColName := tree.Name("cdc_prev")
	if colExists(prevColName) {
		// cdc_prev column exists in the underlying table; unlikely, but okay -- try harder.
		prevColName = "crdb_internal_cdc_prev"
		if colExists(prevColName) {
			// Now, that's entirely unlikely and fatal.
			//
			// It is unfortunate that we have to do these checks. If changefeeds were
			// a "first class citizen", and had a descriptor along with the name,
			// then cdc_prev name could be disambiguated.
			//
			// From https://www.postgresql.org/docs/current/xfunc-sql.html:
			// To use a name, declare the function argument as having a name, and then
			// just write that name in the function body. If the argument name is the
			// same as any column name in the current SQL command within the function,
			// the column name will take precedence. To override this, qualify the
			// argument name with the name of the function itself, that is
			// function_name.argument_name. (If this would conflict with a qualified
			// column name, again the column name wins. You can avoid the ambiguity by
			// choosing a different alias for the table within the SQL command.)
			//
			// If the feed named "myfeed", and a table 'tab' had a column named
			// cdc_prev, then a plain cdc_prev column would be resolved to
			// tab.cdc_prev, and myfeed.cdc_prev can be used to access the previous
			// row.
			// TODO(yevgeniy): rework cdc_prev resolution.
			return nil, pgerror.Newf(pgcode.DuplicateColumn,
				"changefeeds employ an internal, hidden column called cdc_prev, or "+
					"crdb_internal_cdc_prev if cdc_prev already exists. "+
					"Either cdc_prev or crdb_internal_cdc_prev columns must be renamed or dropped from "+
					"the target table %s in order to run changefeed against it", desc.TableName)
		}
	}

	return &prevCol{
		name: prevColName,
		t:    cdcPrevType(desc),
		id:   desc.TableDescriptor().GetNextColumnID(),
	}, nil
}

// cdcPrevType returns a types.T for the tuple corresponding to the
// event descriptor.
func cdcPrevType(desc *cdcevent.EventDescriptor) *types.T {
	numCols := len(desc.ResultColumns())
	tupleTypes := make([]*types.T, 0, numCols)
	tupleLabels := make([]string, 0, numCols)

	for _, c := range desc.ResultColumns() {
		// TODO(yevgeniy): Handle virtual columns in cdc_prev.
		// In order to do this, we have to emit default expression with
		// all named references replaced with tuple field access.
		tupleLabels = append(tupleLabels, c.Name)
		tupleTypes = append(tupleTypes, c.Typ)
	}

	return types.MakeLabeledTuple(tupleTypes, tupleLabels)
}

func (c *prevCol) ColumnDesc() *descpb.ColumnDescriptor {
	if c.d == nil {
		c.initColumnDescriptor()
	}
	return c.d
}

func (c *prevCol) Ordinal() int {
	return int(c.id)
}

func (c *prevCol) Public() bool {
	return true
}

func (c *prevCol) IsHidden() bool {
	return true
}

func (c *prevCol) GetID() descpb.ColumnID {
	return c.id
}

func (c *prevCol) GetName() string {
	return string(c.name)
}

func (c *prevCol) ColName() tree.Name {
	return c.name
}

func (c *prevCol) HasType() bool {
	return true
}

func (c *prevCol) GetType() *types.T {
	return c.t
}

func (c *prevCol) ColumnDescDeepCopy() descpb.ColumnDescriptor {
	return descpb.ColumnDescriptor{}
}

func (c *prevCol) IsMutation() bool {
	return false
}

func (c *prevCol) IsRollback() bool {
	return false
}

func (c *prevCol) MutationID() descpb.MutationID {
	return 0
}

func (c *prevCol) WriteAndDeleteOnly() bool {
	return false
}

func (c *prevCol) DeleteOnly() bool {
	return false
}

func (c *prevCol) Backfilling() bool {
	return false
}

func (c *prevCol) Merging() bool {
	return false
}

func (c *prevCol) Adding() bool {
	return false
}

func (c *prevCol) Dropped() bool {
	return false
}

func (c *prevCol) DeepCopy() catalog.Column {
	return c
}

func (c *prevCol) IsNullable() bool {
	return true
}

func (c *prevCol) HasDefault() bool {
	return false
}

func (c *prevCol) HasNullDefault() bool {
	return false
}

func (c *prevCol) GetDefaultExpr() string {
	return ""
}

func (c *prevCol) HasOnUpdate() bool {
	return false
}

func (c *prevCol) GetOnUpdateExpr() string {
	return ""
}

func (c *prevCol) IsComputed() bool {
	return false
}

func (c *prevCol) GetComputeExpr() string {
	return ""
}

func (c *prevCol) IsInaccessible() bool {
	return false
}

func (c *prevCol) IsExpressionIndexColumn() bool {
	return false
}

func (c *prevCol) NumUsesSequences() int {
	return 0
}

func (c *prevCol) GetUsesSequenceID(usesSequenceOrdinal int) descpb.ID {
	return 0
}

func (c *prevCol) NumOwnsSequences() int {
	return 0
}

func (c *prevCol) NumUsesFunctions() int {
	return 0
}

func (c *prevCol) GetUsesFunctionID(ordinal int) descpb.ID {
	return 0
}

func (c *prevCol) GetOwnsSequenceID(ownsSequenceOrdinal int) descpb.ID {
	return 0
}

func (c *prevCol) IsVirtual() bool {
	return false
}

func (c *prevCol) CheckCanBeInboundFKRef() error {
	return nil
}

func (c *prevCol) CheckCanBeOutboundFKRef() error {
	return nil
}

func (c *prevCol) GetPGAttributeNum() descpb.PGAttributeNum {
	return descpb.PGAttributeNum(c.id)
}

func (c *prevCol) IsSystemColumn() bool {
	return false
}

func (c *prevCol) IsGeneratedAsIdentity() bool {
	return false
}

func (c *prevCol) IsGeneratedAlwaysAsIdentity() bool {
	return false
}

func (c *prevCol) IsGeneratedByDefaultAsIdentity() bool {
	return false
}

func (c *prevCol) GetGeneratedAsIdentityType() catpb.GeneratedAsIdentityType {
	return catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN
}

func (c *prevCol) HasGeneratedAsIdentitySequenceOption() bool {
	return false
}

func (c *prevCol) GetGeneratedAsIdentitySequenceOptionStr() string {
	return ""
}

func (c *prevCol) GetGeneratedAsIdentitySequenceOption(
	defaultIntSize int32,
) (*descpb.TableDescriptor_SequenceOpts, error) {
	return nil, errors.AssertionFailedf("unexpected call to GetGeneratedAsIdentitySequenceOption on cdc_prev")
}

func (c *prevCol) initColumnDescriptor() {
	c.d = &descpb.ColumnDescriptor{
		Name:         c.GetName(),
		ID:           c.id,
		Type:         c.t,
		Nullable:     false,
		Hidden:       true,
		Inaccessible: false,
	}
}
