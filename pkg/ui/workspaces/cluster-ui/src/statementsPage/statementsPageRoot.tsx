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
import { StatementViewType } from "./statementPageTypes";
import { Option } from "src/selectWithDescription/selectWithDescription";
import { SQLActivityContentRoot } from "src/sqlActivityContentRoot/sqlActivityContentRoot";

export type StatementsPageRootProps = {
  activeQueriesView: React.ComponentType;
  fingerprintsView: React.ComponentType;
};

export const StatementsPageRoot = ({
  activeQueriesView,
  fingerprintsView,
}: StatementsPageRootProps): React.ReactElement => {
  const statementOptions: Option[] = [
    {
      value: StatementViewType.ACTIVE,
      label: "Active Executions",
      description:
        "Active executions represent individual statement executions that are " +
        "in progress and have not yet completed.\nThis can help with query " +
        "performance tuning.",
      component: activeQueriesView,
    },
    {
      value: StatementViewType.FINGERPRINTS,
      label: "Statement Fingerprints",
      description:
        "A statement fingerprint represents one or more completed SQL" +
        "statements by replacing literal values (e.g., numbers and strings) with " +
        "underscores (_).\nThis can help you quickly identify frequently executed " +
        "SQL statements and their latencies.",
      component: fingerprintsView,
    },
  ];

  return <SQLActivityContentRoot options={statementOptions} />;
};
