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
import {
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC,
  Duration,
} from "src/util/format";
import { WaitTimeInsightsLabels } from "src/detailsPanels/waitTimeInsightsPanel";
import { NO_SAMPLES_FOUND } from "src/util";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import { WaitTimeDetailsTable } from "./insightDetailsTables";
import {
  BlockedContentionDetails,
  ContentionEvent,
  TxnInsightEvent,
  InsightExecEnum,
  StmtInsightEvent,
} from "../types";

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

type Props = {
  txnDetails: TxnInsightEvent;
  statements: StmtInsightEvent[] | null;
  contentionDetails?: BlockedContentionDetails[];
  setTimeScale: (ts: TimeScale) => void;
};

export const TransactionInsightDetailsOverviewTab: React.FC<Props> = ({
  contentionDetails,
  txnDetails,
  statements,
  setTimeScale,
}) => {
  const [insightsSortSetting, setInsightsSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "insights",
  });
  const isCockroachCloud = useContext(CockroachCloudContext);

  const queryFromStmts = statements?.map(s => s.query)?.join("\n");
  const insightQueries =
    queryFromStmts ?? txnDetails?.query ?? "Insight not found.";
  const insightsColumns = makeInsightsColumns(isCockroachCloud, true);

  const blockingExecutions: ContentionEvent[] = contentionDetails?.map(x => {
    return {
      executionID: x.blockingExecutionID,
      fingerprintID: x.blockingTxnFingerprintID,
      queries: x.blockingQueries,
      startTime: x.collectionTimeStamp,
      contentionTimeMs: x.contentionTimeMs,
      execType: InsightExecEnum.TRANSACTION,
      schemaName: x.schemaName,
      databaseName: x.databaseName,
      tableName: x.tableName,
      indexName: x.indexName,
    };
  });

  const insightRecs = getTxnInsightRecommendations(txnDetails);

  return (
    <div>
      <section className={cx("section")}>
        <Row gutter={24}>
          <Col span={24}>
            <SqlBox value={insightQueries} size={SqlBoxSize.custom} />
          </Col>
        </Row>
        {txnDetails && (
          <>
            <Row gutter={24} type="flex">
              <Col span={12}>
                <SummaryCard>
                  <SummaryCardItem
                    label="Start Time"
                    value={txnDetails.startTime.format(
                      DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC,
                    )}
                  />
                  <SummaryCardItem
                    label="End Time"
                    value={txnDetails.endTime.format(
                      DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC,
                    )}
                  />
                  <SummaryCardItem
                    label="Elapsed Time"
                    value={Duration(txnDetails.elapsedTimeMillis * 1e6)}
                  />
                  <SummaryCardItem
                    label="Rows Read"
                    value={txnDetails.rowsRead}
                  />
                  <SummaryCardItem
                    label="Rows Written"
                    value={txnDetails.rowsWritten}
                  />
                  <SummaryCardItem
                    label="Priority"
                    value={txnDetails.priority ?? NO_SAMPLES_FOUND}
                  />
                </SummaryCard>
              </Col>
              <Col span={12}>
                <SummaryCard>
                  <SummaryCardItem
                    label="Number of Retries"
                    value={txnDetails.retries ?? NO_SAMPLES_FOUND}
                  />
                  {txnDetails.lastRetryReason && (
                    <SummaryCardItem
                      label="Last Retry Reason"
                      value={txnDetails.lastRetryReason}
                    />
                  )}
                  <SummaryCardItem
                    label="Session ID"
                    value={txnDetails.sessionID ?? NO_SAMPLES_FOUND}
                  />
                  <SummaryCardItem
                    label="Application"
                    value={txnDetails.application}
                  />
                  <SummaryCardItem
                    label="Transaction Fingerprint ID"
                    value={TransactionDetailsLink(
                      txnDetails.transactionFingerprintID,
                      txnDetails.startTime,
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
      {blockingExecutions?.length && txnDetails && (
        <section className={tableCx("section")}>
          <Row gutter={24}>
            <Col>
              <Heading type="h5">
                {WaitTimeInsightsLabels.BLOCKED_TXNS_TABLE_TITLE(
                  txnDetails.transactionExecutionID,
                  InsightExecEnum.TRANSACTION,
                )}
              </Heading>
              <div className={tableCx("table-area")}>
                <WaitTimeDetailsTable
                  data={blockingExecutions}
                  execType={InsightExecEnum.TRANSACTION}
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
