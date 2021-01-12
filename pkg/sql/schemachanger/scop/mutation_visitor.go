// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scop

import "context"

// MutationOp is an operation which can be visited by MutationVisitor
type MutationOp interface {
	Op
	Visit(context.Context, MutationVisitor) error
}

// MutationVisitor is a visitor for MutationOp operations.
type MutationVisitor interface {
	MakeDroppedPrimaryIndexDeleteAndWriteOnly(context.Context, MakeDroppedPrimaryIndexDeleteAndWriteOnly) error
	MakeColumnDescriptorPublic(context.Context, MakeColumnDescriptorPublic) error
	MakeAddedColumnDescriptorDeleteOnly(context.Context, MakeAddedColumnDescriptorDeleteOnly) error
	MakeDroppedIndexDeleteOnly(context.Context, MakeDroppedIndexDeleteOnly) error
	AddColumnFamily(context.Context, AddColumnFamily) error
	MakeDroppedNonPrimaryIndexDeleteAndWriteOnly(context.Context, MakeDroppedNonPrimaryIndexDeleteAndWriteOnly) error
	MakeAddedPrimaryIndexPublic(context.Context, MakeAddedPrimaryIndexPublic) error
	MakeDroppedColumnDeleteAndWriteOnly(context.Context, MakeDroppedColumnDeleteAndWriteOnly) error
	MakeAddedIndexDeleteAndWriteOnly(context.Context, MakeAddedIndexDeleteAndWriteOnly) error
	MakeAddedColumnDescriptorDeleteAndWriteOnly(context.Context, MakeAddedColumnDescriptorDeleteAndWriteOnly) error
	MakeDroppedColumnDeleteOnly(context.Context, MakeDroppedColumnDeleteOnly) error
	MakeAddedIndexDeleteOnly(context.Context, MakeAddedIndexDeleteOnly) error
	MakeDroppedColumnDescriptorDeleteAndWriteOnly(context.Context, MakeDroppedColumnDescriptorDeleteAndWriteOnly) error
	MakeIndexAbsent(context.Context, MakeIndexAbsent) error
	AddCheckConstraint(context.Context, AddCheckConstraint) error
	MakeColumnAbsent(context.Context, MakeColumnAbsent) error
}

// Visit is part of the MutationOp interface.
func (op MakeDroppedPrimaryIndexDeleteAndWriteOnly) Visit(
	ctx context.Context, v MutationVisitor,
) error {
	return v.MakeDroppedPrimaryIndexDeleteAndWriteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeColumnDescriptorPublic) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeColumnDescriptorPublic(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeAddedColumnDescriptorDeleteOnly) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeAddedColumnDescriptorDeleteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeDroppedIndexDeleteOnly) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeDroppedIndexDeleteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op AddColumnFamily) Visit(ctx context.Context, v MutationVisitor) error {
	return v.AddColumnFamily(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeDroppedNonPrimaryIndexDeleteAndWriteOnly) Visit(
	ctx context.Context, v MutationVisitor,
) error {
	return v.MakeDroppedNonPrimaryIndexDeleteAndWriteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeAddedPrimaryIndexPublic) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeAddedPrimaryIndexPublic(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeDroppedColumnDeleteAndWriteOnly) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeDroppedColumnDeleteAndWriteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeAddedIndexDeleteAndWriteOnly) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeAddedIndexDeleteAndWriteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeAddedColumnDescriptorDeleteAndWriteOnly) Visit(
	ctx context.Context, v MutationVisitor,
) error {
	return v.MakeAddedColumnDescriptorDeleteAndWriteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeDroppedColumnDeleteOnly) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeDroppedColumnDeleteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeAddedIndexDeleteOnly) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeAddedIndexDeleteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeDroppedColumnDescriptorDeleteAndWriteOnly) Visit(
	ctx context.Context, v MutationVisitor,
) error {
	return v.MakeDroppedColumnDescriptorDeleteAndWriteOnly(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeIndexAbsent) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeIndexAbsent(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op AddCheckConstraint) Visit(ctx context.Context, v MutationVisitor) error {
	return v.AddCheckConstraint(ctx, op)
}

// Visit is part of the MutationOp interface.
func (op MakeColumnAbsent) Visit(ctx context.Context, v MutationVisitor) error {
	return v.MakeColumnAbsent(ctx, op)
}
