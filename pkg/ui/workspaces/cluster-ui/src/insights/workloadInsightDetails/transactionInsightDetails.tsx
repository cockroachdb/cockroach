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
import { Button } from "src/button";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { DATE_FORMAT_24_UTC } from "src/util/format";
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
} from "../types";

import classNames from "classnames/bind";
import { commonStyles } from "src/common";
import insightTableStyles from "src/insightsTable/insightsTable.module.scss";
import { CockroachCloudContext } from "../../contexts";
import { InsightsError } from "../insightsErrorComponent";
import { TransactionDetailsLink } from "../workloadInsights/util";
import { TimeScale } from "../../timeScaleDropdown";

const tableCx = classNames.bind(insightTableStyles);

export interface TransactionInsightDetailsStateProps {
  insightEventDetails: TransactionInsightEventDetailsResponse;
  insightError: Error | null;
}

export interface TransactionInsightDetailsDispatchProps {
  refreshTransactionInsightDetails: (
    req: TransactionInsightEventDetailsRequest,
  ) => void;
  setTimeScale: (ts: TimeScale) => void;
}

export type TransactionInsightDetailsProps =
  TransactionInsightDetailsStateProps &
    TransactionInsightDetailsDispatchProps &
    RouteComponentProps<unknown>;

export class TransactionInsightDetails extends React.Component<TransactionInsightDetailsProps> {
  constructor(props: TransactionInsightDetailsProps) {
    super(props);
  }

  private refresh(): void {
    this.props.refreshTransactionInsightDetails({
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
    const insightDetails = getTransactionInsightEventDetailsFromState(
      this.props.insightEventDetails,
    );
    if (!insightDetails) {
      return null;
    }
    const insightQueries = insightDetails.queries.join("");
    const isCockroachCloud = useContext(CockroachCloudContext);
    const insightsColumns = makeInsightsColumns(isCockroachCloud);

    function insightsTableData(): InsightRecommendation[] {
      const recs: InsightRecommendation[] = [];
      let rec: InsightRecommendation;
      insightDetails.insights.forEach(insight => {
        switch (insight.name) {
          case InsightNameEnum.highContention:
            rec = {
              type: "HighContention",
              details: {
                duration: insightDetails.totalContentionTime,
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
    const blockingExecutions: EventExecution[] =
      insightDetails.blockingContentionDetails.map(x => {
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
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard>
                <SummaryCardItem
                  label="Transaction Fingerprint ID"
                  value={TransactionDetailsLink(
                    insightDetails.fingerprintID,
                    insightDetails.startTime,
                    this.props.setTimeScale,
                  )}
                />
              </SummaryCard>
            </Col>
          </Row>
          <Row gutter={24} className={tableCx("margin-bottom")}>
            {/* TO DO (ericharmeling): We might want this table to span the entire page when other types of insights
            are added*/}
            <Col className="gutter-row" span={12}>
              <InsightsSortedTable columns={insightsColumns} data={tableData} />
            </Col>
          </Row>
        </section>
        <section className={tableCx("section")}>
          <Row gutter={24}>
            <Col className="gutter-row">
              <Heading type="h5">
                {WaitTimeInsightsLabels.BLOCKED_TXNS_TABLE_TITLE(
                  insightDetails.executionID,
                  insightDetails.execType,
                )}
              </Heading>
              <div className={tableCx("margin-bottom-large")}>
                <WaitTimeDetailsTable
                  data={blockingExecutions}
                  execType={insightDetails.execType}
                />
              </div>
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
            renderError={() => InsightsError()}
          />
        </section>
      </div>
    );
  }
}
