// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Col, Row } from "antd";
import "antd/lib/row/style";
import "antd/lib/col/style";
import { Heading } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";

import { SummaryCard, SummaryCardItem } from "../../summaryCard";
// TODO (xinhaoz) we should organize these common page details styles into its own file.
import styles from "../../statementDetails/statementDetails.module.scss";
import { TransactionDetailsLink } from "../workloadInsights/util";
import { ContentionDetails } from "../types";

const cx = classNames.bind(styles);

type Props = {
  conflictDetails: ContentionDetails;
};

const FailedInsightDetailsPanelLabels = {
  SECTION_HEADER: "Failed Execution",
  CONFLICTING_TRANSACTION_HEADER: "Conflicting Transaction",
  CONFLICTING_TRANSACTION_EXEC_ID: "Transaction Execution",
  CONFLICTING_TRANSACTION_FINGERPRINT: "Transaction Fingerprint",
  CONFLICT_LOCATION_HEADER: "Conflict Location",
  DATABASE_NAME: "Database",
  TABLE_NAME: "Table",
  INDEX_NAME: "Index",
};

export const FailedInsightDetailsPanel: React.FC<Props> = ({
  conflictDetails,
}) => {
  return (
    <section
      className={cx("section", "section--container", "margin-bottom-large")}
    >
      <Row gutter={24}>
        <Col>
          <Heading type="h5">
            {FailedInsightDetailsPanelLabels.SECTION_HEADER}
          </Heading>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <Heading type="h5">
                  {
                    FailedInsightDetailsPanelLabels.CONFLICTING_TRANSACTION_HEADER
                  }
                </Heading>
                <SummaryCardItem
                  label={
                    FailedInsightDetailsPanelLabels.CONFLICTING_TRANSACTION_EXEC_ID
                  }
                  value={conflictDetails.blockingExecutionID}
                />
                <SummaryCardItem
                  label={
                    FailedInsightDetailsPanelLabels.CONFLICTING_TRANSACTION_FINGERPRINT
                  }
                  value={TransactionDetailsLink(
                    conflictDetails.blockingTxnFingerprintID,
                  )}
                />
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <Heading type="h5">
                  {FailedInsightDetailsPanelLabels.CONFLICT_LOCATION_HEADER}
                </Heading>
                <SummaryCardItem
                  label={FailedInsightDetailsPanelLabels.DATABASE_NAME}
                  value={conflictDetails.databaseName}
                />
                <SummaryCardItem
                  label={FailedInsightDetailsPanelLabels.TABLE_NAME}
                  value={conflictDetails.tableName}
                />
                <SummaryCardItem
                  label={FailedInsightDetailsPanelLabels.INDEX_NAME}
                  value={conflictDetails.indexName}
                />
              </SummaryCard>
            </Col>
          </Row>
        </Col>
      </Row>
    </section>
  );
};
