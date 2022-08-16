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
import { Heading } from "@cockroachlabs/ui-components";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import moment from "moment";
import { Button } from "src/button";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { Duration } from "src/util";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { getMatchParamByName } from "src/util/query";
import {
  WaitTimeInsightsLabels,
  WaitTimeInsightsPanel,
} from "src/detailsPanels/waitTimeInsightsPanel";
import {
  InsightEventDetailsRequest,
  InsightEventDetailsResponse,
} from "src/api";
import {
  InsightsSortedTable,
  makeInsightsColumns,
} from "src/insightsTable/insightsTable";
import { WaitTimeDetailsTable } from "./insightDetailsTables";
import { getInsightEventDetailsFromState } from "../utils";
import { EventExecution, InsightRecommendation } from "../types";

import classNames from "classnames/bind";
import { commonStyles } from "src/common";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import { CockroachCloudContext } from "../../contexts";
import { InsightsError } from "../insightsErrorComponent";

const tableCx = classNames.bind(insightTableStyles);

export interface InsightDetailsStateProps {
  insightEventDetails: InsightEventDetailsResponse;
  insightError: Error | null;
}

export interface InsightDetailsDispatchProps {
  refreshInsightDetails: (req: InsightEventDetailsRequest) => void;
}

export type InsightDetailsProps = InsightDetailsStateProps &
  InsightDetailsDispatchProps &
  RouteComponentProps<unknown>;

export class InsightDetails extends React.Component<InsightDetailsProps> {
  constructor(props: InsightDetailsProps) {
    super(props);
  }
  private refresh(): void {
    this.props.refreshInsightDetails({
      id: getMatchParamByName(this.props.match, "id"),
    });
  }

  componentDidMount(): void {
    this.refresh();
  }

  componentDidUpdate(): void {
    this.refresh();
  }

  prevPage = (): void => this.props.history.goBack();

  renderContent = (): React.ReactElement => {
    const insightDetails = getInsightEventDetailsFromState(
      this.props.insightEventDetails,
    );
    if (!insightDetails) {
      return null;
    }
    const insightQueries = insightDetails.queries
      .map((query, idx) => {
        if (idx != 0) {
          return "\n" + query;
        } else {
          return query;
        }
      })
      .toString();
    const isCockroachCloud = useContext(CockroachCloudContext);
    const insightsColumns = makeInsightsColumns(isCockroachCloud);
    function insightsTableData(): InsightRecommendation[] {
      const recs = [];
      let rec: InsightRecommendation;
      insightDetails.insights.forEach(insight => {
        switch (insight.name.toString()) {
          case "HIGH_WAIT_TIME":
            rec = {
              type: "HIGH_WAIT_TIME",
              details: {
                duration: insightDetails.elapsedTime,
                description: insight.description,
              },
            };
            break;
        }
      });
      recs.push(rec);
      return recs;
    }
    const tableData = insightsTableData();
    const waitingExecutions: EventExecution[] = [
      {
        executionID: insightDetails.waitingExecutionID,
        fingerprintID: insightDetails.waitingFingerprintID,
        queries: insightDetails.waitingQueries,
        startTime: insightDetails.startTime,
        elapsedTime: insightDetails.elapsedTime,
        execType: insightDetails.execType,
      },
    ];
    return (
      <>
        <section className={tableCx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row" span={24}>
              <SqlBox value={insightQueries} size={SqlBoxSize.custom} />
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
                  label="Elapsed Time"
                  value={Duration(insightDetails.elapsedTime * 1e6)}
                />
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard>
                <SummaryCardItem
                  label="Transaction Fingerprint ID"
                  value={String(insightDetails.fingerprintID)}
                />
              </SummaryCard>
            </Col>
          </Row>
          <Row gutter={24} className={tableCx("margin-bottom")}>
            <InsightsSortedTable columns={insightsColumns} data={tableData} />
          </Row>
        </section>
        <section className={tableCx("section")}>
          <WaitTimeInsightsPanel
            execType={insightDetails.execType}
            executionID={insightDetails.executionID}
            schemaName={insightDetails.schemaName}
            tableName={insightDetails.tableName}
            indexName={insightDetails.indexName}
            databaseName={insightDetails.databaseName}
            contendedKey={String(insightDetails.contendedKey)}
            waitTime={moment.duration(insightDetails.elapsedTime)}
            waitingExecutions={[]}
            blockingExecutions={[]}
          />
          <Row gutter={24}>
            <Col>
              <Row>
                <Heading type="h5">
                  {WaitTimeInsightsLabels.WAITING_TXNS_TABLE_TITLE(
                    insightDetails.executionID,
                    insightDetails.execType,
                  )}
                </Heading>
                <div className={tableCx("margin-bottom-large")}>
                  <WaitTimeDetailsTable
                    data={waitingExecutions}
                    execType={insightDetails.execType}
                  />
                </div>
              </Row>
            </Col>
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
          >{`Transaction Execution ID: ${String(
            getMatchParamByName(this.props.match, "id"),
          )}`}</h3>
        </div>
        <section>
          <Loading
            loading={this.props.insightEventDetails == null}
            page={"Transaction Insight details"}
            error={this.props.insightError}
            render={this.renderContent}
            renderError={() =>
              InsightsError({
                execType: "transaction insights",
              })
            }
          />
        </section>
      </div>
    );
  }
}
