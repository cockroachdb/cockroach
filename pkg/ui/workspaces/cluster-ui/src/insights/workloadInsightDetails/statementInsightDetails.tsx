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
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { Button } from "src/button";
import { Loading } from "src/loading";
import { SqlBox } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { capitalize, Duration } from "src/util";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { getMatchParamByName } from "src/util/query";
import {
  InsightRecommendation,
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import {
  getPriorityFromNumber,
  populateStatemetInsightsFromProblems,
} from "../utils";
import { InsightNameEnum, StatementInsightEvent } from "../types";
import { WorkloadInsightsError } from "../workloadInsights/util";

import classNames from "classnames/bind";
import { commonStyles } from "src/common";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { CockroachCloudContext } from "../../contexts";

const tableCx = classNames.bind(insightTableStyles);
const summaryCardStylesCx = classNames.bind(summaryCardStyles);

export interface StatementInsightDetailsStateProps {
  insightEventDetails: StatementInsightEvent;
  insightError: Error | null;
}

export type StatementInsightDetailsProps = StatementInsightDetailsStateProps &
  RouteComponentProps<unknown>;

export class StatementInsightDetails extends React.Component<StatementInsightDetailsProps> {
  constructor(props: StatementInsightDetailsProps) {
    super(props);
  }
  prevPage = (): void => this.props.history.goBack();

  renderContent = (): React.ReactElement => {
    const insightDetailsArr = [this.props.insightEventDetails];
    populateStatemetInsightsFromProblems(insightDetailsArr);
    const insightDetails = insightDetailsArr[0];
    const isCockroachCloud = useContext(CockroachCloudContext);
    const insightsColumns = makeInsightsColumns(isCockroachCloud);
    function insightsTableData(): InsightRecommendation[] {
      const recs: InsightRecommendation[] = [];
      let rec: InsightRecommendation;

      insightDetails.insights.forEach(insight => {
        switch (insight.name) {
          case InsightNameEnum.highContentionTime:
            rec = {
              type: "HighContentionTime",
              details: {
                duration: insightDetails.elapsedTimeMillis,
                description: insight.description,
              },
            };
            break;
          case InsightNameEnum.failedExecution:
            rec = {
              type: "FAILED",
            };
            break;
          case InsightNameEnum.highRetryCount:
            rec = {
              type: "HighRetryCount",
              execution: {
                retries: insightDetails.retries,
              },
              details: {
                description: insight.description,
              },
            };
            break;
          case InsightNameEnum.planRegression:
            rec = {
              type: "PlanRegression",
              details: {
                description: insight.description,
              },
            };
            break;
          case InsightNameEnum.suboptimalPlan:
            rec = {
              type: "SuboptimalPlan",
              execution: {
                statement: insightDetails.query,
                retries: insightDetails.retries,
                indexRecommendations: insightDetails.indexRecommendations,
              },
              details: {
                description: insight.description,
              },
            };
            break;
          default:
            rec = {
              type: "Unknown",
            };
            break;
        }
        recs.push(rec);
      });
      return recs;
    }
    const tableData = insightsTableData();
    return (
      <>
        <section>
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox value={insightDetails.query} />
            </Col>
          </Row>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard>
                <SummaryCardItem
                  label="Start Time"
                  value={insightDetails.startTime.format(DATE_FORMAT_24_UTC)}
                />
                <SummaryCardItem
                  label="End Time"
                  value={insightDetails.endTime.format(DATE_FORMAT_24_UTC)}
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
                  value={getPriorityFromNumber(Number(insightDetails.priority))}
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
                  value={String(insightDetails.transactionFingerprintID)}
                />
                <SummaryCardItem
                  label="Transaction Execution ID"
                  value={String(insightDetails.transactionID)}
                />
                <SummaryCardItem
                  label="Statement Fingerprint ID"
                  value={String(insightDetails.statementFingerprintID)}
                />
              </SummaryCard>
            </Col>
          </Row>
          <Row gutter={24} className={tableCx("margin-bottom")}>
            <InsightsSortedTable columns={insightsColumns} data={tableData} />
          </Row>
        </section>
      </>
    );
  };

  render(): React.ReactElement {
    return (
      <div>
        <Helmet title={"Details | Insight"} />
        <div>
          <Button
            onClick={this.prevPage}
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
          >{`Statement Execution ID: ${String(
            getMatchParamByName(this.props.match, "id"),
          )}`}</h3>
        </div>
        <section>
          <Loading
            loading={this.props.insightEventDetails == null}
            page={"Transaction Insight details"}
            error={this.props.insightError}
            render={this.renderContent}
            renderError={() => WorkloadInsightsError()}
          />
        </section>
      </div>
    );
  }
}
