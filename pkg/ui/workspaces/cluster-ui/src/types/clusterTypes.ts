// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This explicit typing helps us differentiate between
// node ids and store ids.
export type NodeID = number & { readonly __brand: unique symbol };
export type StoreID = number & { readonly __brand: unique symbol };

export type Region = {
  code: string; // e.g. us-east-1
  label: string;
};
