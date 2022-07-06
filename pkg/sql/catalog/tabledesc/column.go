// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

var _ catalog.Column = (*column)(nil)

// column implements the catalog.Column interface by wrapping the protobuf
// column descriptor along with some metadata from its parent table
// descriptor.
type column struct {
	maybeMutation
	desc    *descpb.ColumnDescriptor
	ordinal int
}

// ColumnDesc returns the underlying protobuf descriptor.
// Ideally, this method should be called as rarely as possible.
func (w column) ColumnDesc() *descpb.ColumnDescriptor {
	return w.desc
}

// ColumnDescDeepCopy returns a deep copy of the underlying protobuf descriptor.
func (w column) ColumnDescDeepCopy() descpb.ColumnDescriptor {
	return *protoutil.Clone(w.desc).(*descpb.ColumnDescriptor)
}

// DeepCopy returns a deep copy of the receiver.
func (w column) DeepCopy() catalog.Column {
	desc := w.ColumnDescDeepCopy()
	return &column{
		maybeMutation: w.maybeMutation,
		desc:          &desc,
		ordinal:       w.ordinal,
	}
}

// Ordinal returns the ordinal of the column in its parent TableDescriptor.
// The ordinal is defined as follows:
// - [:len(desc.Columns)] is the range of public columns,
// - [len(desc.Columns):] is the range of non-public columns.
func (w column) Ordinal() int {
	return w.ordinal
}

// Public returns true iff the column is active, i.e. readable.
func (w column) Public() bool {
	return !w.IsMutation() && !w.IsSystemColumn()
}

// GetID returns the column ID.
func (w column) GetID() descpb.ColumnID {
	return w.desc.ID
}

// GetName returns the column name as a string.
func (w column) GetName() string {
	return w.desc.Name
}

// ColName returns the column name as a tree.Name.
func (w column) ColName() tree.Name {
	return w.desc.ColName()
}

// HasType returns true iff the column type is set.
func (w column) HasType() bool {
	return w.desc.Type != nil
}

// GetType returns the column type.
func (w column) GetType() *types.T {
	return w.desc.Type
}

// IsNullable returns true iff the column allows NULL values.
func (w column) IsNullable() bool {
	return w.desc.Nullable
}

// HasDefault returns true iff the column has a default expression set.
func (w column) HasDefault() bool {
	return w.desc.HasDefault()
}

// GetDefaultExpr returns the column default expression if it exists,
// empty string otherwise.
func (w column) GetDefaultExpr() string {
	if !w.HasDefault() {
		return ""
	}
	return *w.desc.DefaultExpr
}

// HasOnUpdate returns true iff the column has an on update expression set.
func (w column) HasOnUpdate() bool {
	return w.desc.HasOnUpdate()
}

// GetOnUpdateExpr returns the column on update expression if it exists,
// empty string otherwise.
func (w column) GetOnUpdateExpr() string {
	if !w.HasOnUpdate() {
		return ""
	}
	return *w.desc.OnUpdateExpr
}

// IsComputed returns true iff the column is a computed column.
func (w column) IsComputed() bool {
	return w.desc.IsComputed()
}

// GetComputeExpr returns the column computed expression if it exists,
// empty string otherwise.
func (w column) GetComputeExpr() string {
	if !w.IsComputed() {
		return ""
	}
	return *w.desc.ComputeExpr
}

// IsHidden returns true iff the column is not visible.
func (w column) IsHidden() bool {
	return w.desc.Hidden
}

// IsInaccessible returns true iff the column is inaccessible.
func (w column) IsInaccessible() bool {
	return w.desc.Inaccessible
}

// IsExpressionIndexColumn returns true iff the column is an an inaccessible
// virtual computed column that represents an expression in an expression index.
func (w column) IsExpressionIndexColumn() bool {
	return w.IsInaccessible() && w.IsVirtual()
}

// NumUsesSequences returns the number of sequences used by this column.
func (w column) NumUsesSequences() int {
	return len(w.desc.UsesSequenceIds)
}

// GetUsesSequenceID returns the ID of a sequence used by this column.
func (w column) GetUsesSequenceID(usesSequenceOrdinal int) descpb.ID {
	return w.desc.UsesSequenceIds[usesSequenceOrdinal]
}

// NumOwnsSequences returns the number of sequences owned by this column.
func (w column) NumOwnsSequences() int {
	return len(w.desc.OwnsSequenceIds)
}

// GetOwnsSequenceID returns the ID of a sequence owned by this column.
func (w column) GetOwnsSequenceID(ownsSequenceOrdinal int) descpb.ID {
	return w.desc.OwnsSequenceIds[ownsSequenceOrdinal]
}

// IsVirtual returns true iff the column is a virtual column.
func (w column) IsVirtual() bool {
	return w.desc.Virtual
}

// CheckCanBeInboundFKRef returns whether the given column can be on the
// referenced (target) side of a foreign key relation.
func (w column) CheckCanBeInboundFKRef() error {
	return w.desc.CheckCanBeInboundFKRef()
}

// CheckCanBeOutboundFKRef returns whether the given column can be on the
// referencing (origin) side of a foreign key relation.
func (w column) CheckCanBeOutboundFKRef() error {
	return w.desc.CheckCanBeOutboundFKRef()
}

// GetPGAttributeNum returns the PGAttributeNum of the column descriptor
// if the PGAttributeNum is set (non-zero). Returns the ID of the
// column descriptor if the PGAttributeNum is not set.
func (w column) GetPGAttributeNum() descpb.PGAttributeNum {
	return w.desc.GetPGAttributeNum()
}

// IsSystemColumn returns true iff the column is a system column.
func (w column) IsSystemColumn() bool {
	return w.desc.SystemColumnKind != catpb.SystemColumnKind_NONE
}

// IsGeneratedAsIdentity returns true iff the column is created
// with GENERATED {ALWAYS | BY DEFAULT} AS IDENTITY syntax.
func (w column) IsGeneratedAsIdentity() bool {
	return w.desc.GeneratedAsIdentityType != catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN
}

// IsGeneratedAlwaysAsIdentity returns true iff the column is created
// with GENERATED ALWAYS AS IDENTITY syntax.
func (w column) IsGeneratedAlwaysAsIdentity() bool {
	return w.desc.GeneratedAsIdentityType == catpb.GeneratedAsIdentityType_GENERATED_ALWAYS
}

// IsGeneratedByDefaultAsIdentity returns true iff the column is created
// with GENERATED BY DEFAULT AS IDENTITY syntax.
func (w column) IsGeneratedByDefaultAsIdentity() bool {
	return w.desc.GeneratedAsIdentityType == catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT
}

// GetGeneratedAsIdentityType returns the type of how the column was
// created as an IDENTITY column.
// If the column is created with `GENERATED ALWAYS AS IDENTITY` syntax,
// it will return descpb.GeneratedAsIdentityType_GENERATED_ALWAYS;
// if the column is created with `GENERATED BY DEFAULT AS IDENTITY` syntax,
// it will return descpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT;
// otherwise, returns descpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN.
func (w column) GetGeneratedAsIdentityType() catpb.GeneratedAsIdentityType {
	return w.desc.GeneratedAsIdentityType
}

// GetGeneratedAsIdentitySequenceOptionStr returns the string representation
// of the column's `GENERATED AS IDENTITY` sequence option if it exists, empty
// string otherwise.
func (w column) GetGeneratedAsIdentitySequenceOptionStr() string {
	if !w.HasGeneratedAsIdentitySequenceOption() {
		return ""
	}
	return strings.TrimSpace(*w.desc.GeneratedAsIdentitySequenceOption)
}

// GetGeneratedAsIdentitySequenceOption returns the column's `GENERATED AS
// IDENTITY` sequence option if it exists, empty string otherwise.
func (w column) GetGeneratedAsIdentitySequenceOption() *descpb.TableDescriptor_SequenceOpts {
	if !w.HasGeneratedAsIdentitySequenceOption() {
		return nil
	}
	seqOpts, err := transformStringToSeqOption(*w.desc.GeneratedAsIdentitySequenceOption)
	if err != nil {
		return nil
	}
	return seqOpts
}

// transformStringToSeqOption is to transform the sequence options saved the
// descriptor to a descpb.TableDescriptor_SequenceOpts.
// We don't do extra check for the values following the keywords, since they
// should have been checked via sql.go.
func transformStringToSeqOption(s string) (*descpb.TableDescriptor_SequenceOpts, error) {
	var res []tree.SequenceOption
	arr := strings.Fields(strings.ToUpper(s))
	i := 0
	for i < len(arr) {
		switch arr[i] {
		case tree.SeqOptAs:
			res = append(res, tree.SequenceOption{
				Name:          tree.SeqOptAs,
				AsIntegerType: types.ConvertStrToIntType(arr[i+1]),
			})
			i += 2
		case tree.SeqOptCache:
			cacheSize, _ := strconv.ParseInt(arr[i+1], 10, 64)
			res = append(res, tree.SequenceOption{
				Name:   tree.SeqOptCache,
				IntVal: &cacheSize,
			})
			i += 2
		case tree.SeqOptIncrement:
			if arr[i+1] == "BY" {
				i++
			}
			incrementSize, _ := strconv.ParseInt(arr[i+1], 10, 64)
			res = append(res, tree.SequenceOption{
				Name:   tree.SeqOptIncrement,
				IntVal: &incrementSize,
			})
			i += 2
		case tree.SeqOptMinValue:
			minVal, _ := strconv.ParseInt(arr[i+1], 10, 64)
			res = append(res, tree.SequenceOption{
				Name:   tree.SeqOptMinValue,
				IntVal: &minVal,
			})
			i += 2
		case tree.SeqOptMaxValue:
			maxVal, _ := strconv.ParseInt(arr[i+1], 10, 64)
			res = append(res, tree.SequenceOption{
				Name:   tree.SeqOptMaxValue,
				IntVal: &maxVal,
			})
			i += 2
		case tree.SeqOptStart:
			if arr[i+1] == "WITH" {
				i++
			}
			startVal, _ := strconv.ParseInt(arr[i+1], 10, 64)
			res = append(res, tree.SequenceOption{
				Name:   tree.SeqOptStart,
				IntVal: &startVal,
			})
			i += 2
		case tree.SeqOptRestart:
			if arr[i+1] == "WITH" {
				i++
			}
			restartVal, err := strconv.ParseInt(arr[i+1], 10, 64)
			if err != nil {
				// This means it uses the single RESTART keyword, without any int value
				// following.
				res = append(res, tree.SequenceOption{
					Name: tree.SeqOptStart,
				})
				i++
				continue
			}
			res = append(res, tree.SequenceOption{
				Name:   tree.SeqOptStart,
				IntVal: &restartVal,
			})
			i += 2
		case tree.SeqOptVirtual:
			res = append(res, tree.SequenceOption{
				Name: tree.SeqOptVirtual,
			})
			i++
		case "NO":
			switch arr[i+1] {
			case tree.SeqOptCycle:
				res = append(res, tree.SequenceOption{
					Name: tree.SeqOptNoCycle,
				})
			case tree.SeqOptMinValue:
				res = append(res, tree.SequenceOption{
					Name: tree.SeqOptMinValue,
				})
			case tree.SeqOptMaxValue:
				res = append(res, tree.SequenceOption{
					Name: tree.SeqOptMaxValue,
				})
			}
			i += 2
		default:
			i++
		}
	}
	opts := &descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
	}
	if err := AssignSequenceOptions(
		opts,
		res,
		true, /* setDefaults */
		nil,  /* existingType */
	); err != nil {
		return nil, err
	}

	return opts, nil
}

// HasGeneratedAsIdentitySequenceOption returns true if there is a
// customized sequence option when this column is created as a
// `GENERATED AS IDENTITY` column.
func (w column) HasGeneratedAsIdentitySequenceOption() bool {
	return w.desc.GeneratedAsIdentitySequenceOption != nil
}

// columnCache contains precomputed slices of catalog.Column interfaces.
type columnCache struct {
	all                  []catalog.Column
	public               []catalog.Column
	writable             []catalog.Column
	deletable            []catalog.Column
	nonDrop              []catalog.Column
	visible              []catalog.Column
	accessible           []catalog.Column
	readable             []catalog.Column
	withUDTs             []catalog.Column
	system               []catalog.Column
	familyDefaultColumns []descpb.IndexFetchSpec_FamilyDefaultColumn
	index                []indexColumnCache
}

type indexColumnCache struct {
	all          []catalog.Column
	allDirs      []catpb.IndexColumn_Direction
	key          []catalog.Column
	keyDirs      []catpb.IndexColumn_Direction
	stored       []catalog.Column
	keySuffix    []catalog.Column
	full         []catalog.Column
	fullDirs     []catpb.IndexColumn_Direction
	keyAndSuffix []descpb.IndexFetchSpec_KeyColumn
}

// newColumnCache returns a fresh fully-populated columnCache struct for the
// TableDescriptor.
func newColumnCache(desc *descpb.TableDescriptor, mutations *mutationCache) *columnCache {
	c := columnCache{}
	// Build a slice of structs to back the public and system interfaces in c.all.
	// This is better than allocating memory once per struct.
	numPublic := len(desc.Columns)
	backingStructs := make([]column, numPublic, numPublic+len(colinfo.AllSystemColumnDescs))
	for i := range desc.Columns {
		backingStructs[i] = column{desc: &desc.Columns[i], ordinal: i}
	}
	numMutations := len(mutations.columns)
	numDeletable := numPublic + numMutations
	for i := range colinfo.AllSystemColumnDescs {
		col := column{
			desc:    &colinfo.AllSystemColumnDescs[i],
			ordinal: numDeletable + i,
		}
		backingStructs = append(backingStructs, col)
	}
	// Populate the c.all slice with Column interfaces.
	c.all = make([]catalog.Column, 0, numDeletable+len(colinfo.AllSystemColumnDescs))
	for i := range backingStructs[:numPublic] {
		c.all = append(c.all, &backingStructs[i])
	}
	for _, m := range mutations.columns {
		c.all = append(c.all, m.AsColumn())
	}
	for i := range backingStructs[numPublic:] {
		c.all = append(c.all, &backingStructs[numPublic+i])
	}
	// Populate the remaining column slice fields.
	c.deletable = c.all[:numDeletable]
	c.system = c.all[numDeletable:]
	c.public = c.all[:numPublic]
	if numMutations == 0 {
		c.readable = c.public
		c.writable = c.public
		c.nonDrop = c.public
	} else {
		for _, col := range c.deletable {
			if !col.DeleteOnly() {
				lazyAllocAppendColumn(&c.writable, col, numDeletable)
			}
			if !col.Dropped() {
				lazyAllocAppendColumn(&c.nonDrop, col, numDeletable)
			}
			lazyAllocAppendColumn(&c.readable, col, numDeletable)
		}
	}
	for _, col := range c.deletable {
		if col.Public() && !col.IsHidden() && !col.IsInaccessible() {
			lazyAllocAppendColumn(&c.visible, col, numPublic)
		}
		if col.Public() && !col.IsInaccessible() {
			lazyAllocAppendColumn(&c.accessible, col, numPublic)
		}
		if col.HasType() && col.GetType().UserDefined() {
			lazyAllocAppendColumn(&c.withUDTs, col, numDeletable)
		}
	}

	// Populate familyDefaultColumns.
	for i := range desc.Families {
		if f := &desc.Families[i]; f.DefaultColumnID != 0 {
			if c.familyDefaultColumns == nil {
				c.familyDefaultColumns = make([]descpb.IndexFetchSpec_FamilyDefaultColumn, 0, len(desc.Families)-i)
			}
			c.familyDefaultColumns = append(c.familyDefaultColumns, descpb.IndexFetchSpec_FamilyDefaultColumn{
				FamilyID:        f.ID,
				DefaultColumnID: f.DefaultColumnID,
			})
		}
	}

	// Populate the per-index column cache
	c.index = make([]indexColumnCache, 0, 1+len(desc.Indexes)+len(mutations.indexes))
	c.index = append(c.index, makeIndexColumnCache(&desc.PrimaryIndex, c.all))
	for i := range desc.Indexes {
		c.index = append(c.index, makeIndexColumnCache(&desc.Indexes[i], c.all))
	}
	for i := range mutations.indexes {
		c.index = append(c.index, makeIndexColumnCache(mutations.indexes[i].AsIndex().IndexDesc(), c.all))
	}
	return &c
}

// makeIndexColumnCache builds a cache of catalog.Column slices pertaining to
// the columns referenced in an index.
func makeIndexColumnCache(idx *descpb.IndexDescriptor, all []catalog.Column) (ic indexColumnCache) {
	nKey := len(idx.KeyColumnIDs)
	nKeySuffix := len(idx.KeySuffixColumnIDs)
	nStored := len(idx.StoreColumnIDs)
	nAll := nKey + nKeySuffix + nStored
	ic.allDirs = make([]catpb.IndexColumn_Direction, nAll)
	// Only copy key column directions, others will remain at ASC (default value).
	copy(ic.allDirs, idx.KeyColumnDirections)
	ic.all = make([]catalog.Column, 0, nAll)
	appendColumnsByID(&ic.all, all, idx.KeyColumnIDs)
	appendColumnsByID(&ic.all, all, idx.KeySuffixColumnIDs)
	appendColumnsByID(&ic.all, all, idx.StoreColumnIDs)
	ic.key = ic.all[:nKey]
	ic.keyDirs = ic.allDirs[:nKey]
	ic.keySuffix = ic.all[nKey : nKey+nKeySuffix]
	ic.stored = ic.all[nKey+nKeySuffix:]
	nFull := nKey
	if !idx.Unique {
		nFull = nFull + nKeySuffix
	}
	ic.full = ic.all[:nFull]
	ic.fullDirs = ic.allDirs[:nFull]

	// Populate keyAndSuffix. Note that this method can be called on an incomplete
	// (mutable) descriptor (e.g. as part of initializing a new descriptor); this
	// code needs to tolerate any descriptor state (like having no key columns, or
	// having uninitialized column IDs).
	var invertedColumnID descpb.ColumnID
	if nKey > 0 && idx.Type == descpb.IndexDescriptor_INVERTED {
		invertedColumnID = idx.InvertedColumnID()
	}
	var compositeIDs catalog.TableColSet
	for _, colID := range idx.CompositeColumnIDs {
		compositeIDs.Add(colID)
	}
	ic.keyAndSuffix = make([]descpb.IndexFetchSpec_KeyColumn, nKey+nKeySuffix)
	for i := range ic.keyAndSuffix {
		col := ic.all[i]
		if col == nil {
			ic.keyAndSuffix[i].Name = "invalid"
			continue
		}
		colID := col.GetID()
		typ := col.GetType()
		if colID != 0 && colID == invertedColumnID {
			typ = idx.InvertedColumnKeyType()
		}
		ic.keyAndSuffix[i] = descpb.IndexFetchSpec_KeyColumn{
			IndexFetchSpec_Column: descpb.IndexFetchSpec_Column{
				Name:          col.GetName(),
				ColumnID:      colID,
				Type:          typ,
				IsNonNullable: !col.IsNullable(),
			},
			Direction:   ic.allDirs[i],
			IsComposite: compositeIDs.Contains(colID),
			IsInverted:  colID == invertedColumnID,
		}
	}
	return ic
}

func appendColumnsByID(slice *[]catalog.Column, source []catalog.Column, ids []descpb.ColumnID) {
	for _, id := range ids {
		var col catalog.Column
		for _, candidate := range source {
			if candidate.GetID() == id {
				col = candidate
				break
			}
		}
		*slice = append(*slice, col)
	}
}

func lazyAllocAppendColumn(slice *[]catalog.Column, col catalog.Column, cap int) {
	if *slice == nil {
		*slice = make([]catalog.Column, 0, cap)
	}
	*slice = append(*slice, col)
}

func getSequenceIntegerBounds(
	integerType *types.T,
) (lowerIntBound int64, upperIntBound int64, err error) {
	switch integerType {
	case types.Int2:
		return math.MinInt16, math.MaxInt16, nil
	case types.Int4:
		return math.MinInt32, math.MaxInt32, nil
	case types.Int:
		return math.MinInt64, math.MaxInt64, nil
	}

	return 0, 0, errors.AssertionFailedf(
		"CREATE SEQUENCE option AS received type %s, must be integer",
		integerType,
	)
}

func setSequenceIntegerBounds(
	opts *descpb.TableDescriptor_SequenceOpts,
	integerType *types.T,
	isAscending bool,
	setMinValue bool,
	setMaxValue bool,
) error {
	var minValue int64 = math.MinInt64
	var maxValue int64 = math.MaxInt64

	if isAscending {
		minValue = 1

		switch integerType {
		case types.Int2:
			maxValue = math.MaxInt16
		case types.Int4:
			maxValue = math.MaxInt32
		case types.Int:
			// Do nothing, it's the default.
		default:
			return errors.AssertionFailedf(
				"CREATE SEQUENCE option AS received type %s, must be integer",
				integerType,
			)
		}
	} else {
		maxValue = -1
		switch integerType {
		case types.Int2:
			minValue = math.MinInt16
		case types.Int4:
			minValue = math.MinInt32
		case types.Int:
			// Do nothing, it's the default.
		default:
			return errors.AssertionFailedf(
				"CREATE SEQUENCE option AS received type %s, must be integer",
				integerType,
			)
		}
	}
	if setMinValue {
		opts.MinValue = minValue
	}
	if setMaxValue {
		opts.MaxValue = maxValue
	}
	return nil
}

// AssignSequenceOptions moves options from the AST node to the sequence options descriptor,
// starting with defaults and overriding them with user-provided options.
func AssignSequenceOptions(
	opts *descpb.TableDescriptor_SequenceOpts,
	optsNode tree.SequenceOptions,
	setDefaults bool,
	existingType *types.T,
) error {
	wasAscending := opts.Increment > 0

	// Set the default integer type of a sequence.
	var integerType = types.Int
	// All other defaults are dependent on the value of increment
	// and the AS integerType. (i.e. whether the sequence is ascending
	// or descending, bigint vs. smallint)
	for _, option := range optsNode {
		if option.Name == tree.SeqOptIncrement {
			opts.Increment = *option.IntVal
		} else if option.Name == tree.SeqOptAs {
			integerType = option.AsIntegerType
			opts.AsIntegerType = integerType.SQLString()
		}
	}
	if opts.Increment == 0 {
		return errors.New("INCREMENT must not be zero")
	}
	isAscending := opts.Increment > 0

	// Set increment-dependent defaults.
	if setDefaults {
		if isAscending {
			opts.MinValue = 1
			opts.MaxValue = math.MaxInt64
			opts.Start = opts.MinValue
		} else {
			opts.MinValue = math.MinInt64
			opts.MaxValue = -1
			opts.Start = opts.MaxValue
		}
		opts.CacheSize = 1
	}

	lowerIntBound, upperIntBound, err := getSequenceIntegerBounds(integerType)
	if err != nil {
		return err
	}

	// Set default MINVALUE and MAXVALUE if AS option value for integer type is specified.
	if opts.AsIntegerType != "" {
		// We change MINVALUE and MAXVALUE if it is the originally set to the default during ALTER.
		setMinValue := setDefaults
		setMaxValue := setDefaults
		if !setDefaults && existingType != nil {
			existingLowerIntBound, existingUpperIntBound, err := getSequenceIntegerBounds(existingType)
			if err != nil {
				return err
			}
			if (wasAscending && opts.MinValue == 1) || (!wasAscending && opts.MinValue == existingLowerIntBound) {
				setMinValue = true
			}
			if (wasAscending && opts.MaxValue == existingUpperIntBound) || (!wasAscending && opts.MaxValue == -1) {
				setMaxValue = true
			}
		}

		if err := setSequenceIntegerBounds(
			opts,
			integerType,
			isAscending,
			setMinValue,
			setMaxValue,
		); err != nil {
			return err
		}
	}

	// Fill in all other options.
	var restartVal *int64
	optionsSeen := map[string]bool{}
	for _, option := range optsNode {
		// Error on duplicate options.
		_, seenBefore := optionsSeen[option.Name]
		if seenBefore {
			return errors.New("conflicting or redundant options")
		}
		optionsSeen[option.Name] = true

		switch option.Name {
		case tree.SeqOptCycle:
			return unimplemented.NewWithIssue(20961,
				"CYCLE option is not supported")
		case tree.SeqOptNoCycle:
			// Do nothing; this is the default.
		case tree.SeqOptCache:
			if v := *option.IntVal; v >= 1 {
				opts.CacheSize = v
			} else {
				return errors.Newf(
					"CACHE (%d) must be greater than zero", v)
			}
		case tree.SeqOptIncrement:
			// Do nothing; this has already been set.
		case tree.SeqOptMinValue:
			// A value of nil represents the user explicitly saying `NO MINVALUE`.
			if option.IntVal != nil {
				opts.MinValue = *option.IntVal
			}
		case tree.SeqOptMaxValue:
			// A value of nil represents the user explicitly saying `NO MAXVALUE`.
			if option.IntVal != nil {
				opts.MaxValue = *option.IntVal
			}
		case tree.SeqOptStart:
			opts.Start = *option.IntVal
		case tree.SeqOptRestart:
			// The RESTART option does not get saved, but still gets validated below.
			restartVal = option.IntVal
		case tree.SeqOptVirtual:
			opts.Virtual = true
		}
	}

	if setDefaults || (wasAscending && opts.Start == 1) || (!wasAscending && opts.Start == -1) {
		// If start option not specified, set it to MinValue (for ascending sequences)
		// or MaxValue (for descending sequences).
		// We only do this if we're setting it for the first time, or the sequence was
		// ALTERed with the default original values.
		if _, startSeen := optionsSeen[tree.SeqOptStart]; !startSeen {
			if opts.Increment > 0 {
				opts.Start = opts.MinValue
			} else {
				opts.Start = opts.MaxValue
			}
		}
	}

	if opts.MinValue < lowerIntBound {
		return errors.Newf(
			"MINVALUE (%d) must be greater than (%d) for type %s",
			opts.MinValue,
			lowerIntBound,
			integerType.SQLString(),
		)
	}
	if opts.MaxValue < lowerIntBound {
		return errors.Newf(
			"MAXVALUE (%d) must be greater than (%d) for type %s",
			opts.MaxValue,
			lowerIntBound,
			integerType.SQLString(),
		)
	}
	if opts.MinValue > upperIntBound {
		return errors.Newf(
			"MINVALUE (%d) must be less than (%d) for type %s",
			opts.MinValue,
			upperIntBound,
			integerType.SQLString(),
		)
	}
	if opts.MaxValue > upperIntBound {
		return errors.Newf(
			"MAXVALUE (%d) must be less than (%d) for type %s",
			opts.MaxValue,
			upperIntBound,
			integerType.SQLString(),
		)
	}
	if opts.Start > opts.MaxValue {
		return errors.Newf(
			"START value (%d) cannot be greater than MAXVALUE (%d)",
			opts.Start,
			opts.MaxValue,
		)
	}
	if opts.Start < opts.MinValue {
		return errors.Newf(
			"START value (%d) cannot be less than MINVALUE (%d)",
			opts.Start,
			opts.MinValue,
		)
	}
	if restartVal != nil {
		if *restartVal > opts.MaxValue {
			return errors.Newf(
				"RESTART value (%d) cannot be greater than MAXVALUE (%d)",
				*restartVal,
				opts.MaxValue,
			)
		}
		if *restartVal < opts.MinValue {
			return errors.Newf(
				"RESTART value (%d) cannot be less than MINVALUE (%d)",
				*restartVal,
				opts.MinValue,
			)
		}
	}
	return nil
}
