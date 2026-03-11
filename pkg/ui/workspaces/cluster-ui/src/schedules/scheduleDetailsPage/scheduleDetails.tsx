// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import Long from "long";
import React from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { getSchedule } from "src/api/schedulesApi";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { Loading } from "src/loading";
import scheduleStyles from "src/schedules/schedules.module.scss";
import { SqlBox, SqlBoxSize } from "src/sql";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import {
  DATE_FORMAT_24_TZ,
  idAttr,
  getMatchParamByName,
  useSwrWithClusterId,
} from "src/util";

import { Timestamp } from "../../timestamp";

const cardCx = classNames.bind(summaryCardStyles);
const scheduleCx = classNames.bind(scheduleStyles);

export type ScheduleDetailsProps = RouteComponentProps;

export const ScheduleDetails: React.FC<ScheduleDetailsProps> = props => {
  const idStr = getMatchParamByName(props.match, idAttr);

  const {
    data: schedule,
    error,
    isLoading,
  } = useSwrWithClusterId({ name: "schedule", id: idStr }, () =>
    getSchedule(Long.fromString(idStr)),
  );

  const prevPage = (): void => props.history.goBack();

  const renderContent = (): React.ReactElement => {
    return (
      <>
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SqlBox value={schedule.command} size={SqlBoxSize.CUSTOM} />
          </Col>
        </Row>
        <Row gutter={24}>
          <Col className="gutter-row" span={12}>
            <SummaryCard>
              <SummaryCardItem label="Label" value={schedule.label} />
              <SummaryCardItem label="Status" value={schedule.status} />
              <SummaryCardItem label="State" value={schedule.state} />
            </SummaryCard>
          </Col>
          <Col className="gutter-row" span={12}>
            <SummaryCard className={cardCx("summary-card")}>
              <SummaryCardItem
                label="Creation Time"
                value={
                  schedule.created ? (
                    <Timestamp
                      time={schedule.created}
                      format={DATE_FORMAT_24_TZ}
                    />
                  ) : (
                    <>N/A</>
                  )
                }
              />
              <SummaryCardItem
                label="Next Execution Time"
                value={
                  schedule.nextRun ? (
                    <Timestamp
                      time={schedule.nextRun}
                      format={DATE_FORMAT_24_TZ}
                    />
                  ) : (
                    <>N/A</>
                  )
                }
              />
              <SummaryCardItem label="Recurrence" value={schedule.recurrence} />
              <SummaryCardItem
                label="Jobs Running"
                value={String(schedule.jobsRunning)}
              />
              <SummaryCardItem label="Owner" value={schedule.owner} />
            </SummaryCard>
          </Col>
        </Row>
      </>
    );
  };

  return (
    <div className={scheduleCx("schedule-details")}>
      <Helmet title={"Details | Schedule"} />
      <div className={scheduleCx("section page--header")}>
        <Button
          onClick={prevPage}
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
        >{`Schedule ID: ${idStr}`}</h3>
      </div>
      <section className={scheduleCx("section section--container")}>
        <Loading
          loading={!schedule || isLoading}
          page={"schedule details"}
          error={error}
          render={renderContent}
        />
      </section>
    </div>
  );
};
