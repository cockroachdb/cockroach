// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Heading } from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import React, { useContext, useState } from "react";

import { TxnInsightDetailsReqErrs } from "src/api";
import { WaitTimeInsightsLabels } from "src/detailsPanels/waitTimeInsightsPanel";
import insightsDetailsStyles from "src/insights/workloadInsightDetails/insightsDetails.module.scss";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { NO_SAMPLES_FOUND } from "src/util";
import {
  Count,
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ,
  Duration,
} from "src/util/format";

import { CockroachCloudContext } from "../../contexts";
import { SortSetting } from "../../sortedtable";
import { TimeScale } from "../../timeScaleDropdown";
import { Timestamp } from "../../timestamp";
import { InsightsError } from "../insightsErrorComponent";
import {
  ContentionDetails,
  ContentionEvent,
  InsightExecEnum,
  InsightNameEnum,
  StmtInsightEvent,
  TxnInsightEvent,
} from "../types";
import { getTxnInsightRecommendations } from "../utils";
import { TransactionDetailsLink } from "../workloadInsights/util";

import { FailedInsightDetailsPanel } from "./failedInsightDetailsPanel";
import { WaitTimeDetailsTable } from "./insightDetailsTables";

const cx = classNames.bind(insightsDetailsStyles);
const tableCx = classNames.bind(insightTableStyles);

type Props = {
  txnDetails: TxnInsightEvent | null;
  statements: StmtInsightEvent[] | null;
  contentionDetails?: ContentionDetails[];
  setTimeScale: (ts: TimeScale) => void;
  hasAdminRole: boolean;
  errors: TxnInsightDetailsReqErrs | null;
  maxRequestsReached: boolean;
  maxApiSizeReached: boolean;
};

export const TransactionInsightDetailsOverviewTab: React.FC<Props> = ({
  errors,
  contentionDetails,
  txnDetails,
  statements,
  setTimeScale,
  hasAdminRole,
  maxRequestsReached,
}) => {
  const [insightsSortSetting, setInsightsSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "insights",
  });
  const isCockroachCloud = useContext(CockroachCloudContext);

  const queryFromStmts = statements?.map(s => s.query)?.join("\n");
  let insightQueries = queryFromStmts?.length
    ? queryFromStmts
    : txnDetails?.query ?? "Insight not found.";
  if (maxRequestsReached) {
    insightQueries = `${insightQueries} \n\nNot all statements are displayed because 
the maximum number of statements was reached in the console.`;
  }
  const insightsColumns = makeInsightsColumns(
    isCockroachCloud,
    hasAdminRole,
    true,
  );

  const blockingExecutions: ContentionEvent[] = contentionDetails
    ?.filter(e => e.contentionType === "LOCK_WAIT")
    .map(event => {
      const stmtInsight = statements.find(
        stmt => stmt.statementExecutionID === event.waitingStmtID,
      );
      return {
        executionID: event.blockingExecutionID,
        fingerprintID: event.blockingTxnFingerprintID,
        waitingStmtID: event.waitingStmtID,
        waitingStmtFingerprintID: event.waitingStmtFingerprintID,
        queries: event.blockingTxnQuery,
        startTime: event.collectionTimeStamp,
        contentionTimeMs: event.contentionTimeMs,
        execType: InsightExecEnum.TRANSACTION,
        schemaName: event.schemaName,
        databaseName: event.databaseName,
        tableName: event.tableName,
        indexName: event.indexName,
        stmtInsightEvent: stmtInsight,
      };
    });

  // We only expect up to 1 serialization conflict since only 1 can be recorded
  // per execution.
  const serializationConflict = contentionDetails?.find(
    e => e.contentionType === "SERIALIZATION_CONFLICT",
  );

  const insightRecs = getTxnInsightRecommendations(txnDetails);
  const hasContentionInsights =
    txnDetails?.insights.find(
      i => i.name === InsightNameEnum.HIGH_CONTENTION,
    ) != null;

  return (
    <div>
      <section className={cx("section")}>
        <Loading
          loading={!maxRequestsReached && txnDetails == null}
          page="Transaction Details"
          error={errors?.txnDetailsErr}
          renderError={() => InsightsError(errors?.txnDetailsErr?.message)}
        >
          <Row gutter={24}>
            <Col span={24}>
              <SqlBox
                value={insightQueries}
                size={SqlBoxSize.CUSTOM}
                format={true}
              />
            </Col>
          </Row>
          {txnDetails && (
            <>
              <Row gutter={24}>
                <Col span={12}>
                  <SummaryCard>
                    <SummaryCardItem
                      label="Start Time"
                      value={
                        <Timestamp
                          time={txnDetails.startTime}
                          format={
                            DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ
                          }
                        />
                      }
                    />
                    <SummaryCardItem
                      label="End Time"
                      value={
                        <Timestamp
                          time={txnDetails.endTime}
                          format={
                            DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ
                          }
                        />
                      }
                    />
                    <SummaryCardItem
                      label="Elapsed Time"
                      value={Duration(txnDetails.elapsedTimeMillis * 1e6)}
                    />
                    <SummaryCardItem
                      label="SQL CPU Time"
                      value={Duration(txnDetails.cpuSQLNanos)}
                    />
                    <SummaryCardItem
                      label="Rows Read"
                      value={Count(txnDetails.rowsRead)}
                    />
                    <SummaryCardItem
                      label="Rows Written"
                      value={Count(txnDetails.rowsWritten)}
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
                      value={Count(txnDetails.retries) ?? NO_SAMPLES_FOUND}
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
                        txnDetails.application,
                      )}
                    />
                  </SummaryCard>
                </Col>
              </Row>
              <Row gutter={24}>
                <Col span={24}>
                  <InsightsSortedTable
                    columns={insightsColumns}
                    data={insightRecs}
                    sortSetting={insightsSortSetting}
                    onChangeSortSetting={setInsightsSortSetting}
                    tableWrapperClassName={tableCx("sorted-table")}
                  />
                </Col>
              </Row>
            </>
          )}
        </Loading>
      </section>
      {serializationConflict && (
        <FailedInsightDetailsPanel conflictDetails={serializationConflict} />
      )}
      {hasContentionInsights && (
        <Loading
          loading={!maxRequestsReached && contentionDetails == null}
          page="Transaction Details"
          error={errors?.contentionErr}
          renderError={() => InsightsError(errors?.contentionErr?.message)}
        >
          <section className={tableCx("section")}>
            <Row gutter={24}>
              <Col>
                <Heading type="h5">
                  {WaitTimeInsightsLabels.blockedTxnsTableTitle(
                    txnDetails?.transactionExecutionID,
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
        </Loading>
      )}
    </div>
  );
};
