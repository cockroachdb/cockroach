// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Heading } from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import "antd/lib/col/style";
import React from "react";

// TODO (xinhaoz) we should organize these common page details styles into its own file.
import styles from "../../statementDetails/statementDetails.module.scss";
import { SummaryCard, SummaryCardItem } from "../../summaryCard";
import { ContentionDetails } from "../types";
import { TransactionDetailsLink } from "../workloadInsights/util";
import "antd/lib/row/style";

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
