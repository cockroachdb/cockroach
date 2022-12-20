// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext, useState } from "react";
import { Heading } from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC } from "src/util/format";
import { WaitTimeInsightsLabels } from "src/detailsPanels/waitTimeInsightsPanel";
import { NO_SAMPLES_FOUND } from "src/util";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import { WaitTimeDetailsTable } from "./insightDetailsTables";
import { ContentionEvent, TxnInsightDetails } from "../types";

import classNames from "classnames/bind";
import { CockroachCloudContext } from "../../contexts";
import { TransactionDetailsLink } from "../workloadInsights/util";
import { TimeScale } from "../../timeScaleDropdown";
import { getTxnInsightRecommendations } from "../utils";
import { SortSetting } from "../../sortedtable";

import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import insightsDetailsStyles from "src/insights/workloadInsightDetails/insightsDetails.module.scss";

const cx = classNames.bind(insightsDetailsStyles);
const tableCx = classNames.bind(insightTableStyles);

export interface TransactionInsightDetailsStateProps {
  insightDetails: TxnInsightDetails;
  insightError: Error | null;
}

export interface TransactionInsightDetailsDispatchProps {
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
  const [insightsSortSetting, setInsightsSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "insights",
  });
  const isCockroachCloud = useContext(CockroachCloudContext);

  const insightQueries =
    insightDetails?.queries?.join("") || "Insight not found.";
  const insightsColumns = makeInsightsColumns(
    isCockroachCloud,
    insightDetails.queries?.length > 1,
    true,
  );

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
    stmtInsights?.reduce((count, stmt) => (count += stmt.rowsRead), 0) ??
    NO_SAMPLES_FOUND;
  const rowsWritten =
    stmtInsights?.reduce((count, stmt) => (count += stmt.rowsWritten), 0) ??
    NO_SAMPLES_FOUND;

  return (
    <div>
      <section className={cx("section")}>
        <Row gutter={24}>
          <Col span={24}>
            <SqlBox value={insightQueries} size={SqlBoxSize.custom} />
          </Col>
        </Row>
        {insightDetails && (
          <>
            <Row gutter={24} type="flex">
              <Col span={12}>
                <SummaryCard>
                  <SummaryCardItem
                    label="Start Time"
                    value={
                      insightDetails.startTime?.format(
                        DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC,
                      ) ?? NO_SAMPLES_FOUND
                    }
                  />
                  <SummaryCardItem label="Rows Read" value={rowsRead} />
                  <SummaryCardItem label="Rows Written" value={rowsWritten} />
                  <SummaryCardItem
                    label="Priority"
                    value={insightDetails.priority ?? NO_SAMPLES_FOUND}
                  />
                  <SummaryCardItem
                    label="Full Scan"
                    value={
                      insightDetails.statementInsights
                        ?.some(stmt => stmt.isFullScan)
                        ?.toString() ?? NO_SAMPLES_FOUND
                    }
                  />
                </SummaryCard>
              </Col>
              <Col span={12}>
                <SummaryCard>
                  <SummaryCardItem
                    label="Number of Retries"
                    value={insightDetails.retries ?? NO_SAMPLES_FOUND}
                  />
                  {insightDetails.lastRetryReason && (
                    <SummaryCardItem
                      label="Last Retry Reason"
                      value={insightDetails.lastRetryReason}
                    />
                  )}
                  <SummaryCardItem
                    label="Session ID"
                    value={insightDetails.sessionID ?? NO_SAMPLES_FOUND}
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
              <Col span={24}>
                <InsightsSortedTable
                  columns={insightsColumns}
                  data={insightRecs}
                  sortSetting={insightsSortSetting}
                  onChangeSortSetting={setInsightsSortSetting}
                />
              </Col>
            </Row>
          </>
        )}
      </section>
      {blockingExecutions?.length && insightDetails && (
        <section className={tableCx("section")}>
          <Row gutter={24}>
            <Col>
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
                  setTimeScale={setTimeScale}
                />
              </div>
            </Col>
          </Row>
        </section>
      )}
    </div>
  );
};
