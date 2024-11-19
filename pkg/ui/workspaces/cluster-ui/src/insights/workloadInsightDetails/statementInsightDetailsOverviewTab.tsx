// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { Heading } from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import React, { useContext, useMemo, useState } from "react";

import insightsDetailsStyles from "src/insights/workloadInsightDetails/insightsDetails.module.scss";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { capitalize, Duration } from "src/util";
import {
  Count,
  DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ,
} from "src/util/format";
// Styles

import { CockroachCloudContext } from "../../contexts";
import { WaitTimeInsightsLabels } from "../../detailsPanels/waitTimeInsightsPanel";
import { SortSetting } from "../../sortedtable";
import { Timestamp } from "../../timestamp";
import { StmtInsightEvent } from "../types";
import { getStmtInsightRecommendations } from "../utils";
import {
  StatementDetailsLink,
  TransactionDetailsLink,
} from "../workloadInsights/util";

import { ContentionStatementDetailsTable } from "./insightDetailsTables";

const cx = classNames.bind(insightsDetailsStyles);
const tableCx = classNames.bind(insightTableStyles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

export interface StatementInsightDetailsOverviewTabProps {
  insightEventDetails: StmtInsightEvent;
  hasAdminRole: boolean;
}

export const StatementInsightDetailsOverviewTab: React.FC<
  StatementInsightDetailsOverviewTabProps
> = ({ insightEventDetails, hasAdminRole }) => {
  const isCockroachCloud = useContext(CockroachCloudContext);

  const insightsColumns = useMemo(
    () => makeInsightsColumns(isCockroachCloud, hasAdminRole),
    [isCockroachCloud, hasAdminRole],
  );

  const insightDetails = insightEventDetails;
  const tableData = getStmtInsightRecommendations(insightDetails);

  const [
    insightsDetailsContentionSortSetting,
    setDetailsContentionSortSetting,
  ] = useState<SortSetting>({
    ascending: false,
    columnTitle: "duration",
  });
  let contentionTable: JSX.Element = null;
  if (insightDetails?.contentionEvents != null) {
    contentionTable = (
      <Row gutter={24} className={tableCx("margin-bottom")}>
        <Col className="gutter-row">
          <Heading type="h5">
            {WaitTimeInsightsLabels.blockedTxnsTableTitle(
              insightDetails?.statementExecutionID,
              "statement",
            )}
          </Heading>
          <ContentionStatementDetailsTable
            data={insightDetails?.contentionEvents}
            sortSetting={insightsDetailsContentionSortSetting}
            onChangeSortSetting={setDetailsContentionSortSetting}
          />
        </Col>
      </Row>
    );
  }

  const [insightsSortSetting, setInsightsSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "insights",
  });

  return (
    <section className={cx("section")}>
      <Row gutter={24}>
        <Col span={12}>
          <SummaryCard>
            <SummaryCardItem
              label="Start Time"
              value={
                <Timestamp
                  time={insightDetails?.startTime}
                  format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
                />
              }
            />
            <SummaryCardItem
              label="End Time"
              value={
                <Timestamp
                  time={insightDetails?.endTime}
                  format={DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_TZ}
                />
              }
            />
            <SummaryCardItem
              label="Elapsed Time"
              value={Duration(insightDetails?.elapsedTimeMillis * 1e6)}
            />
            <SummaryCardItem
              label={"SQL CPU Time"}
              value={Duration(insightDetails?.cpuSQLNanos)}
            />
            <SummaryCardItem
              label="Rows Read"
              value={Count(insightDetails?.rowsRead)}
            />
            <SummaryCardItem
              label="Rows Written"
              value={Count(insightDetails?.rowsWritten)}
            />
            <SummaryCardItem
              label="Transaction Priority"
              value={capitalize(insightDetails?.priority)}
            />
            <SummaryCardItem
              label="Full Scan"
              value={capitalize(String(insightDetails?.isFullScan))}
            />
          </SummaryCard>
        </Col>
        <Col className="gutter-row" span={12}>
          <SummaryCard>
            <SummaryCardItem
              label="Transaction Retries"
              value={Count(insightDetails?.retries)}
            />
            {insightDetails?.lastRetryReason && (
              <SummaryCardItem
                label="Last Retry Reason"
                value={insightDetails?.lastRetryReason.toString()}
              />
            )}
            <p className={summaryCardStylesCx("summary--card__divider")} />
            <SummaryCardItem
              label="Session ID"
              value={String(insightDetails?.sessionID)}
            />
            <SummaryCardItem
              label="Transaction Fingerprint ID"
              value={TransactionDetailsLink(
                insightDetails?.transactionFingerprintID,
                insightDetails?.application,
              )}
            />
            <SummaryCardItem
              label="Transaction Execution ID"
              value={String(insightDetails?.transactionExecutionID)}
            />
            <SummaryCardItem
              label="Statement Fingerprint ID"
              value={StatementDetailsLink(insightDetails)}
            />
          </SummaryCard>
        </Col>
      </Row>
      <Row gutter={24}>
        <Col span={24}>
          <InsightsSortedTable
            sortSetting={insightsSortSetting}
            onChangeSortSetting={setInsightsSortSetting}
            columns={insightsColumns}
            data={tableData}
            tableWrapperClassName={tableCx("sorted-table")}
          />
        </Col>
      </Row>
      {contentionTable}
    </section>
  );
};
