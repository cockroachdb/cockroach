// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { TransactionViewType } from "./transactionsPageTypes";
import { Option } from "src/selectWithDescription/selectWithDescription";
import { SQLActivityContentRoot } from "src/sqlActivityContentRoot/sqlActivityContentRoot";

export type TransactionsPageRootProps = {
  activeTransactionsView: React.ComponentType;
  fingerprintsView: React.ComponentType;
};

export const TransactionsPageRoot = ({
  activeTransactionsView,
  fingerprintsView,
}: TransactionsPageRootProps): React.ReactElement => {
  const transactionOptions: Option[] = [
    {
      value: TransactionViewType.ACTIVE,
      label: "Active Executions",
      description:
        "Active executions represent individual transaction executions that are in " +
        "progress or recently completed.\n This can help with query performance tuning.",
      component: activeTransactionsView,
    },
    {
      value: TransactionViewType.FINGERPRINTS,
      label: "Transaction Fingerprints",
      description:
        "A transaction fingerprint represents statement fingerprints grouped by transaction. " +
        "Statement fingerprints represent one or more SQL statements by replacing literal " +
        "values (e.g., numbers and strings) with underscores (_). This can help you quickly " +
        "identify frequently executed SQL transactions and their latencies.",
      component: fingerprintsView,
    },
  ];

  return <SQLActivityContentRoot options={transactionOptions} />;
};
