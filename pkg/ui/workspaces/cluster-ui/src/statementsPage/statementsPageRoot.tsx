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
import { SQLActivityRootControls } from "src/sqlActivityRootControls/sqlActivityRootControls";
import {
  StatementsPage,
  StatementsPageProps,
} from "src/statementsPage/statementsPage";
import {
  ActiveStatementsView,
  ActiveStatementsViewProps,
} from "./activeStatementsView";
import { statementsSql } from "src/util/docs";
import { Anchor } from "src/anchor";

export type StatementsPageRootProps = {
  fingerprintsPageProps: StatementsPageProps;
  activePageProps: ActiveStatementsViewProps;
};

export const StatementsPageRoot = ({
  fingerprintsPageProps,
  activePageProps,
}: StatementsPageRootProps): React.ReactElement => {
  const statementOptions: Option[] = [
    {
      value: StatementViewType.FINGERPRINTS,
      label: "Statement Fingerprints",
      description: (
        <span>
          {`A statement fingerprint represents one or more completed SQL
          statements by replacing literal values (e.g., numbers and strings)
          with underscores (_).\nThis can help you quickly identify
          frequently executed SQL statements and their latencies. `}
          <Anchor href={statementsSql}>Learn more</Anchor>
        </span>
      ),
      component: <StatementsPage {...fingerprintsPageProps} />,
    },
    {
      value: StatementViewType.ACTIVE,
      label: "Active Executions",
      description: (
        <span>
          Active executions represent individual statement executions in
          progress. Use active statement execution details, such as the
          application or elapsed time, to understand and tune workload
          performance.
          {/* TODO (xinhaoz) #78379 add 'Learn More' link to documentation page*/}
        </span>
      ),
      component: <ActiveStatementsView {...activePageProps} />,
    },
  ];

  return <SQLActivityRootControls options={statementOptions} />;
};
