// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React, { useContext, useEffect, useState } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Heading } from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { Button } from "src/button";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC } from "src/util/format";
import { getMatchParamByName } from "src/util/query";
import { WaitTimeInsightsLabels } from "src/detailsPanels/waitTimeInsightsPanel";
import {
  TransactionInsightEventDetailsRequest,
  TransactionInsightEventDetailsResponse,
} from "src/api";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import { WaitTimeDetailsTable } from "./insightDetailsTables";
import { getTransactionInsightEventDetailsFromState } from "../utils";
import {
  EventExecution,
  InsightNameEnum,
  InsightRecommendation,
  TransactionInsightEventDetails,
} from "../types";
import { SortSetting } from "../../sortedtable";

import classNames from "classnames/bind";
import { commonStyles } from "src/common";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import { CockroachCloudContext } from "../../contexts";
import { InsightsError } from "../insightsErrorComponent";
import { TransactionDetailsLink } from "../workloadInsights/util";
import { TimeScale } from "../../timeScaleDropdown";
import { idAttr } from "src/util";

const tableCx = classNames.bind(insightTableStyles);

function insightsTableData(
  insightDetails: TransactionInsightEventDetails,
): InsightRecommendation[] {
  if (!insightDetails?.insights) {
    return [];
  }
  return insightDetails.insights
    .filter(insight => insight.name === InsightNameEnum.highContention)
    .map(insight => {
      return {
        type: "HighContention",
        details: {
          duration: insightDetails.totalContentionTime,
          description: insight.description,
        },
      };
    });
}

export interface TransactionInsightDetailsStateProps {
  insightEventDetails: TransactionInsightEventDetailsResponse;
  insightError: Error | null;
  hasAdminRole: boolean;
}

export interface TransactionInsightDetailsDispatchProps {
  refreshTransactionInsightDetails: (
    req: TransactionInsightEventDetailsRequest,
  ) => void;
  setTimeScale: (ts: TimeScale) => void;
  refreshUserSQLRoles: () => void;
}

export type TransactionInsightDetailsProps =
  TransactionInsightDetailsStateProps &
    TransactionInsightDetailsDispatchProps &
    RouteComponentProps<unknown>;

export const TransactionInsightDetails: React.FC<
  TransactionInsightDetailsProps
> = ({
  refreshTransactionInsightDetails,
  setTimeScale,
  history,
  insightEventDetails,
  insightError,
  match,
  hasAdminRole,
  refreshUserSQLRoles,
}) => {
  const [insightsSortSetting, setInsightsSortSetting] = useState<SortSetting>({
    ascending: false,
    columnTitle: "insights",
  });
  const isCockroachCloud = useContext(CockroachCloudContext);
  const executionID = getMatchParamByName(match, idAttr);
  const noInsights = !insightEventDetails;
  useEffect(() => {
    refreshUserSQLRoles();
    if (noInsights) {
      refreshTransactionInsightDetails({
        id: executionID,
      });
    }
  }, [
    executionID,
    refreshTransactionInsightDetails,
    noInsights,
    refreshUserSQLRoles,
  ]);

  const prevPage = (): void => history.goBack();

  const insightDetails =
    getTransactionInsightEventDetailsFromState(insightEventDetails);

  const insightQueries =
    insightDetails?.queries.join("") || "Insight not found.";
  const insightsColumns = makeInsightsColumns(isCockroachCloud, hasAdminRole);

  const blockingExecutions: EventExecution[] =
    insightDetails?.blockingContentionDetails.map(x => {
      return {
        executionID: x.blockingExecutionID,
        fingerprintID: x.blockingFingerprintID,
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

  const tableData = insightsTableData(insightDetails);

  return (
    <div>
      <Helmet title={"Details | Insight"} />
      <div>
        <Button
          onClick={prevPage}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
          className={commonStyles("small-margin")}
        >
          Insights
        </Button>
        <h3
          className={commonStyles("base-heading", "no-margin-bottom")}
        >{`Transaction Execution ID: ${String(
          getMatchParamByName(match, idAttr),
        )}`}</h3>
      </div>
      <section>
        <Loading
          loading={!insightDetails}
          page={"Transaction Insight details"}
          error={insightError}
          renderError={() => InsightsError()}
        >
          <section className={tableCx("section")}>
            <Row gutter={24}>
              <Col className="gutter-row" span={24}>
                <SqlBox value={insightQueries} size={SqlBoxSize.custom} />
              </Col>
            </Row>
            {insightDetails && (
              <>
                <Row gutter={24} type="flex">
                  <Col className="gutter-row" span={12}>
                    <SummaryCard>
                      <SummaryCardItem
                        label="Start Time"
                        value={insightDetails.startTime.format(
                          DATE_WITH_SECONDS_AND_MILLISECONDS_FORMAT_24_UTC,
                        )}
                      />
                    </SummaryCard>
                  </Col>
                  <Col className="gutter-row" span={12}>
                    <SummaryCard>
                      <SummaryCardItem
                        label="Transaction Fingerprint ID"
                        value={TransactionDetailsLink(
                          insightDetails.fingerprintID,
                          insightDetails.startTime,
                          setTimeScale,
                        )}
                      />
                    </SummaryCard>
                  </Col>
                </Row>
                <Row gutter={24} className={tableCx("margin-bottom")}>
                  {/* TO DO (ericharmeling): We might want this table to span the entire page when other types of insights
            are added*/}
                  <Col className="gutter-row" span={12}>
                    <InsightsSortedTable
                      columns={insightsColumns}
                      data={tableData}
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
                <Col className="gutter-row">
                  <Heading type="h5">
                    {WaitTimeInsightsLabels.BLOCKED_TXNS_TABLE_TITLE(
                      insightDetails.executionID,
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
        </Loading>
      </section>
    </div>
  );
};
