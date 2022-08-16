// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import Long from "long";
import React from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import { ScheduleRequest, ScheduleResponse } from "src/api/schedulesApi";
import { Button } from "src/button";
import { Loading } from "src/loading";
import { SqlBox } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import { TimestampToMoment } from "src/util";
import { DATE_FORMAT_24_UTC } from "src/util/format";
import { getMatchParamByName } from "src/util/query";

import { commonStyles } from "src/common";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import scheduleStyles from "src/schedules/schedules.module.scss";

import classNames from "classnames/bind";

const cardCx = classNames.bind(summaryCardStyles);
const scheduleCx = classNames.bind(scheduleStyles);

export interface ScheduleDetailsStateProps {
  schedule: ScheduleResponse;
  scheduleError: Error | null;
  scheduleLoading: boolean;
}

export interface ScheduleDetailsDispatchProps {
  refreshSchedule: (req: ScheduleRequest) => void;
}

export type ScheduleDetailsProps = ScheduleDetailsStateProps &
  ScheduleDetailsDispatchProps &
  RouteComponentProps<unknown>;

export class ScheduleDetails extends React.Component<ScheduleDetailsProps> {
  constructor(props: ScheduleDetailsProps) {
    super(props);
  }

  private refresh(): void {
    this.props.refreshSchedule(
      new cockroach.server.serverpb.ScheduleRequest({
        schedule_id: Long.fromString(
          getMatchParamByName(this.props.match, "id"),
        ),
      }),
    );
  }

  componentDidMount(): void {
    this.refresh();
  }

  componentDidUpdate(): void {
    this.refresh();
  }

  prevPage = (): void => this.props.history.goBack();

  renderContent = (): React.ReactElement => {
    const schedule = this.props.schedule;
    return (
      <>
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SqlBox
              value={JSON.stringify(JSON.parse(schedule.command), null, 2)}
            />
          </Col>
        </Row>
        <Row gutter={24}>
          <Col className="gutter-row" span={12}>
            <SummaryCard>
              <SummaryCardItem label="Label" value={schedule.label} />
              <SummaryCardItem
                label="Status"
                value={schedule.schedule_status}
              />
              <SummaryCardItem label="State" value={schedule.state} />
            </SummaryCard>
          </Col>
          <Col className="gutter-row" span={12}>
            <SummaryCard className={cardCx("summary-card")}>
              <SummaryCardItem
                label="Creation Time"
                value={TimestampToMoment(schedule.created).format(
                  DATE_FORMAT_24_UTC,
                )}
              />
              <SummaryCardItem
                label="Next Execution Time"
                value={TimestampToMoment(schedule.next_run).format(
                  DATE_FORMAT_24_UTC,
                )}
              />
              <SummaryCardItem label="Recurrence" value={schedule.recurrence} />
              <SummaryCardItem
                label="Jobs Running"
                value={String(schedule.jobs_running)}
              />
              <SummaryCardItem label="Owner" value={schedule.owner} />
            </SummaryCard>
          </Col>
        </Row>
      </>
    );
  };

  render(): React.ReactElement {
    const isLoading = !this.props.schedule || this.props.scheduleLoading;
    const error = this.props.schedule && this.props.scheduleError;
    return (
      <div className={scheduleCx("schedule-details")}>
        <Helmet title={"Details | Schedule"} />
        <div className={scheduleCx("section page--header")}>
          <Button
            onClick={this.prevPage}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
            className={commonStyles("small-margin")}
          >
            Schedules
          </Button>
          <h3
            className={scheduleCx("page--header__title")}
          >{`Schedule ID: ${String(
            getMatchParamByName(this.props.match, "id"),
          )}`}</h3>
        </div>
        <section className={scheduleCx("section section--container")}>
          <Loading
            loading={isLoading}
            page={"schedule details"}
            error={error}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }
}
