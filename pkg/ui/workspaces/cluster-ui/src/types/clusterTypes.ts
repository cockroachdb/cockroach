// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This explicit typing helps us differentiate between
// node ids and store ids.
export type NodeID = number;

export type Region = {
  code: string; // e.g. us-east-1
  label: string;
};
