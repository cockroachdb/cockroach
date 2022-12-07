// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React, { useContext, useMemo, useState } from "react";
import { Col, Row } from "antd";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { capitalize, Duration } from "src/util";
import { DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC } from "src/util/format";
import { FlattenedStmtInsightEvent } from "../types";
import classNames from "classnames/bind";
import { CockroachCloudContext } from "../../contexts";

// Styles
import insightsDetailsStyles from "src/insights/workloadInsightDetails/insightsDetails.module.scss";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import "antd/lib/col/style";
import "antd/lib/row/style";
import {
  StatementDetailsLink,
  TransactionDetailsLink,
} from "../workloadInsights/util";
import { TimeScale } from "../../timeScaleDropdown";
import { getStmtInsightRecommendations } from "../utils";
import { ContentionStatementDetailsTable } from "./insightDetailsTables";
import { WaitTimeInsightsLabels } from "../../detailsPanels/waitTimeInsightsPanel";
import { Heading } from "@cockroachlabs/ui-components";
import { SortSetting } from "../../sortedtable";

const cx = classNames.bind(insightsDetailsStyles);
const tableCx = classNames.bind(insightTableStyles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

export interface StatementInsightDetailsOverviewTabProps {
  insightEventDetails: FlattenedStmtInsightEvent;
  setTimeScale: (ts: TimeScale) => void;
}

export const StatementInsightDetailsOverviewTab: React.FC<
  StatementInsightDetailsOverviewTabProps
> = ({ insightEventDetails, setTimeScale }) => {
  const isCockroachCloud = useContext(CockroachCloudContext);

  const insightsColumns = useMemo(
    () => makeInsightsColumns(isCockroachCloud),
    [isCockroachCloud],
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
  if (insightDetails.contentionEvents != null) {
    contentionTable = (
      <Row gutter={24} className={tableCx("margin-bottom")}>
        <Col className="gutter-row">
          <Heading type="h5">
            {WaitTimeInsightsLabels.BLOCKED_TXNS_TABLE_TITLE(
              insightDetails.statementExecutionID,
              "statement",
            )}
          </Heading>
          <ContentionStatementDetailsTable
            data={insightDetails.contentionEvents}
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
      <Row gutter={24} type="flex">
        <Col span={12}>
          <SummaryCard>
            <SummaryCardItem
              label="Start Time"
              value={insightDetails.startTime.format(
                DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC,
              )}
            />
            <SummaryCardItem
              label="End Time"
              value={insightDetails.endTime.format(
                DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC,
              )}
            />
            <SummaryCardItem
              label="Elapsed Time"
              value={Duration(insightDetails.elapsedTimeMillis * 1e6)}
            />
            <SummaryCardItem
              label="Rows Read"
              value={insightDetails.rowsRead}
            />
            <SummaryCardItem
              label="Rows Written"
              value={insightDetails.rowsWritten}
            />
            <SummaryCardItem
              label="Transaction Priority"
              value={capitalize(insightDetails.priority)}
            />
            <SummaryCardItem
              label="Full Scan"
              value={capitalize(String(insightDetails.isFullScan))}
            />
          </SummaryCard>
        </Col>
        <Col className="gutter-row" span={12}>
          <SummaryCard>
            <SummaryCardItem
              label="Transaction Retries"
              value={insightDetails.retries}
            />
            {insightDetails.lastRetryReason && (
              <SummaryCardItem
                label="Last Retry Reason"
                value={insightDetails.lastRetryReason.toString()}
              />
            )}
            <p className={summaryCardStylesCx("summary--card__divider")} />
            <SummaryCardItem
              label="Session ID"
              value={String(insightDetails.sessionID)}
            />
            <SummaryCardItem
              label="Transaction Fingerprint ID"
              value={TransactionDetailsLink(
                insightDetails.transactionFingerprintID,
                insightDetails.startTime,
                setTimeScale,
              )}
            />
            <SummaryCardItem
              label="Transaction Execution ID"
              value={String(insightDetails.transactionExecutionID)}
            />
            <SummaryCardItem
              label="Statement Fingerprint ID"
              value={StatementDetailsLink(insightDetails, setTimeScale)}
            />
          </SummaryCard>
        </Col>
      </Row>
      <Row gutter={24} className={tableCx("margin-bottom")}>
        <Col>
          <InsightsSortedTable
            sortSetting={insightsSortSetting}
            onChangeSortSetting={setInsightsSortSetting}
            columns={insightsColumns}
            data={tableData}
          />
        </Col>
      </Row>
      {contentionTable}
    </section>
  );
};
