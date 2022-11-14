// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext } from "react";
import { Heading } from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { WaitTimeInsightsLabels } from "src/detailsPanels/waitTimeInsightsPanel";
import { TxnContentionInsightDetailsRequest } from "src/api";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import { WaitTimeDetailsTable } from "./insightDetailsTables";
import { ContentionEvent, TxnInsightDetails } from "../types";

import classNames from "classnames/bind";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import { CockroachCloudContext } from "../../contexts";
import { TransactionDetailsLink } from "../workloadInsights/util";
import { TimeScale } from "../../timeScaleDropdown";
import { getTxnInsightRecommendations } from "../utils";

const tableCx = classNames.bind(insightTableStyles);

export interface TransactionInsightDetailsStateProps {
  insightDetails: TxnInsightDetails;
  insightError: Error | null;
}

export interface TransactionInsightDetailsDispatchProps {
  refreshTransactionInsightDetails: (
    req: TxnContentionInsightDetailsRequest,
  ) => void;
  setTimeScale: (ts: TimeScale) => void;
}

type Props = {
  insightDetails: TxnInsightDetails;
  setTimeScale: (ts: TimeScale) => void;
};

export const TransactionInsightDetailsOverviewTab: React.FC<Props> = ({
  insightDetails,
  setTimeScale,
}) => {
  const isCockroachCloud = useContext(CockroachCloudContext);

  const insightQueries =
    insightDetails?.queries?.join("") || "Insight not found.";
  const insightsColumns = makeInsightsColumns(isCockroachCloud);

  const blockingExecutions: ContentionEvent[] =
    insightDetails?.blockingContentionDetails?.map(x => {
      return {
        executionID: x.blockingExecutionID,
        fingerprintID: x.blockingTxnFingerprintID,
        queries: x.blockingQueries,
        startTime: x.collectionTimeStamp,
        contentionTimeMs: x.contentionTimeMs,
        execType: insightDetails.execType,
        schemaName: x.schemaName,
        databaseName: x.databaseName,
        tableName: x.tableName,
        indexName: x.indexName,
      };
    });

  const stmtInsights = insightDetails?.statementInsights?.map(stmt => ({
    ...stmt,
    retries: insightDetails.retries,
    databaseName: insightDetails.databaseName,
  }));

  // Build insight recommendations off of all stmt insights.
  // TODO: (xinhaoz) these recs should be a bit more detailed when there
  // is stmt info available
  const insightRecs = getTxnInsightRecommendations(insightDetails);

  const rowsRead =
    stmtInsights?.reduce((count, stmt) => (count += stmt.rowsRead), 0) ?? "N/A";
  const rowsWritten =
    stmtInsights?.reduce((count, stmt) => (count += stmt.rowsWritten), 0) ??
    "N/A";

  return (
    <div>
      <section className={tableCx("section")}>
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SqlBox value={insightQueries} size={SqlBoxSize.custom} />
          </Col>
        </Row>
        {insightDetails && (
          <>
            <Row gutter={24}>
              <Col className="gutter-row" span={12}>
                <SummaryCard>
                  <SummaryCardItem
                    label="Start Time"
                    value={
                      insightDetails.startTime?.format(DATE_FORMAT_24_UTC) ??
                      "N/A"
                    }
                  />
                  <SummaryCardItem label="Rows Read" value={rowsRead} />
                  <SummaryCardItem label="Rows Written" value={rowsWritten} />
                  <SummaryCardItem
                    label="Priority"
                    value={insightDetails.priority ?? "N/A"}
                  />
                  <SummaryCardItem
                    label="Full Scan"
                    value={
                      insightDetails.statementInsights
                        ?.some(stmt => stmt.isFullScan)
                        ?.toString() ?? "N/A"
                    }
                  />
                </SummaryCard>
              </Col>
              <Col className="gutter-row" span={12}>
                <SummaryCard>
                  <SummaryCardItem
                    label="Number of Retries"
                    value={insightDetails.retries ?? "N/A"}
                  />
                  <SummaryCardItem
                    label="Last Retry Reason"
                    value={insightDetails.lastRetryReason ?? "N/A"}
                  />
                  <SummaryCardItem
                    label="Session ID"
                    value={insightDetails.sessionID ?? "N/A"}
                  />
                  <SummaryCardItem
                    label="Application"
                    value={insightDetails.application}
                  />
                  <SummaryCardItem
                    label="Transaction Fingerprint ID"
                    value={TransactionDetailsLink(
                      insightDetails.transactionFingerprintID,
                      insightDetails.startTime,
                      setTimeScale,
                    )}
                  />
                </SummaryCard>
              </Col>
            </Row>
            <Row gutter={24} className={tableCx("margin-bottom")}>
              <Col className="gutter-row" span={24}>
                <InsightsSortedTable
                  columns={insightsColumns}
                  data={insightRecs}
                />
              </Col>
            </Row>
          </>
        )}
      </section>
      {blockingExecutions?.length && insightDetails && (
        <section className={tableCx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row">
              <Heading type="h5">
                {WaitTimeInsightsLabels.BLOCKED_TXNS_TABLE_TITLE(
                  insightDetails.transactionExecutionID,
                  insightDetails.execType,
                )}
              </Heading>
              <div className={tableCx("table-area")}>
                <WaitTimeDetailsTable
                  data={blockingExecutions}
                  execType={insightDetails.execType}
                />
              </div>
            </Col>
          </Row>
        </section>
      )}
    </div>
  );
};
