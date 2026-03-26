// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ActiveTransactionDetails } from "./activeTransactionDetails";

export const RecentTransactionDetailsPageConnected =
  ActiveTransactionDetails;

// Prior to 23.1, this component was called
// ActiveTransactionDetailsPageConnected. We keep the alias here to avoid
// breaking the multi-version support in managed-service's console code.
// When managed-service drops support for 22.2 (around the end of 2024?),
// we can remove this code.
export const ActiveTransactionDetailsPageConnected =
  RecentTransactionDetailsPageConnected;
