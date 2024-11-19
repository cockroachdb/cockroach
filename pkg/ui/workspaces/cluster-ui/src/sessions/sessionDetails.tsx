// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row } from "antd";
import classNames from "classnames/bind";
import isNil from "lodash/isNil";
import moment from "moment-timezone";
import React from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { commonStyles } from "src/common";
import { SqlBox, SqlBoxSize } from "src/sql/box";
import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
import { NodeLink } from "src/statementsTable/statementsTableContent";
import { UIConfigState } from "src/store";
import {
  ICancelQueryRequest,
  ICancelSessionRequest,
} from "src/store/terminateQuery";
import { createTimeScaleFromDateRange, TimeScale } from "src/timeScaleDropdown";
import { sessionAttr } from "src/util/constants";
import { DurationToMomentDuration, TimestampToMoment } from "src/util/convert";
import { Bytes, DATE_FORMAT_24_TZ, Count } from "src/util/format";
import { getMatchParamByName } from "src/util/query";

import { Button } from "../button";
import { CircleFilled } from "../icon";
import { Loading } from "../loading";
import LoadingError from "../sqlActivity/errorComponent";
import { SummaryCard, SummaryCardItem } from "../summaryCard";
import { Text, TextTypes } from "../text";
import { Timestamp } from "../timestamp";
import { FixLong } from "../util";

import styles from "./sessionDetails.module.scss";
import {
  getStatusClassname,
  getStatusString,
  SessionInfo,
} from "./sessionsTable";
import TerminateQueryModal, {
  TerminateQueryModalRef,
} from "./terminateQueryModal";
import TerminateSessionModal, {
  TerminateSessionModalRef,
} from "./terminateSessionModal";

const cx = classNames.bind(styles);
const statementsPageCx = classNames.bind(statementsPageStyles);

export interface OwnProps {
  id?: string;
  nodeNames: { [nodeId: string]: string };
  session: SessionInfo;
  sessionError: Error | null;
  refreshSessions: () => void;
  refreshNodes: () => void;
  refreshNodesLiveness: () => void;
  cancelSession: (payload: ICancelSessionRequest) => void;
  cancelQuery: (payload: ICancelQueryRequest) => void;
  uiConfig?: UIConfigState["pages"]["sessionDetails"];
  isTenant?: UIConfigState["isTenant"];
  onBackButtonClick?: () => void;
  onTerminateSessionClick?: () => void;
  onTerminateStatementClick?: () => void;
  onStatementClick?: () => void;
  setTimeScale: (ts: TimeScale) => void;
}

export type SessionDetailsProps = OwnProps & RouteComponentProps;

function yesOrNo(b: boolean) {
  return b ? "Yes" : "No";
}

export const MemoryUsageItem: React.FC<{
  allocBytes: Long;
  maxAllocBytes: Long;
}> = ({ allocBytes, maxAllocBytes }) => (
  <SummaryCardItem
    label={"Memory Usage"}
    value={
      Bytes(FixLong(allocBytes ?? 0).toNumber()) +
      "/" +
      Bytes(FixLong(maxAllocBytes ?? 0).toNumber())
    }
  />
);

export class SessionDetails extends React.Component<SessionDetailsProps> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;

  componentDidMount(): void {
    if (!this.props.isTenant) {
      this.props.refreshNodes();
      this.props.refreshNodesLiveness();
    }
    this.props.refreshSessions();
  }

  componentDidUpdate(): void {
    // Normally, we would refresh the sessions here, but we don't want to
    // have the per-session page update whenever our data source updates
    // because in real workloads, sessions change what they're doing very
    // regularly, leading to a confusing and too-fast-refreshing page
    // experience for people trying to understand what is happening in a
    // particular session.
    // this.props.refreshSessions();
  }

  constructor(props: SessionDetailsProps) {
    super(props);
    this.terminateSessionRef = React.createRef();
    this.terminateQueryRef = React.createRef();
  }

  backToSessionsPage = (): void => {
    const { history, onBackButtonClick } = this.props;
    onBackButtonClick && onBackButtonClick();
    history.push("/sql-activity?tab=Sessions");
  };

  render(): React.ReactElement {
    const sessionID = getMatchParamByName(this.props.match, sessionAttr);
    const {
      sessionError,
      cancelSession,
      cancelQuery,
      onTerminateSessionClick,
      onTerminateStatementClick,
    } = this.props;
    const session = this.props.session?.session;
    const showActionButtons = !!session && !sessionError;
    return (
      <div className={cx("sessions-details")}>
        <Helmet title={`Details | ${sessionID} | Sessions`} />
        <div className={`${statementsPageCx("section")} ${cx("page--header")}`}>
          <Button
            onClick={this.backToSessionsPage}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
            className="small-margin"
          >
            Sessions
          </Button>
          <div className={cx("heading-with-controls")}>
            <h3
              className={`${commonStyles("base-heading")} ${cx(
                "page--header__title",
              )}`}
            >
              Session Details
            </h3>
            {showActionButtons && (
              <div className={cx("heading-controls-group")}>
                <Button
                  disabled={session.active_queries?.length === 0}
                  onClick={() => {
                    onTerminateStatementClick && onTerminateStatementClick();
                    if (session.active_queries?.length > 0) {
                      this.terminateQueryRef?.current?.showModalFor({
                        query_id: session.active_queries[0].id,
                        node_id: session.node_id.toString(),
                      });
                    }
                  }}
                  type="secondary"
                  size="small"
                >
                  Cancel Statement
                </Button>
                <Button
                  onClick={() => {
                    onTerminateSessionClick && onTerminateSessionClick();
                    this.terminateSessionRef?.current?.showModalFor({
                      session_id: session.id,
                      node_id: session.node_id.toString(),
                    });
                  }}
                  type="secondary"
                  size="small"
                >
                  Cancel Session
                </Button>
              </div>
            )}
          </div>
        </div>
        <section
          className={`${statementsPageCx("section")} ${cx(
            "section--container",
          )}`}
        >
          <Loading
            loading={isNil(this.props.session)}
            page={"sessions details"}
            error={this.props.sessionError}
            render={this.renderContent}
            renderError={() =>
              LoadingError({
                statsType: "sessions",
                error: this.props.sessionError,
              })
            }
          />
        </section>
        <TerminateSessionModal
          ref={this.terminateSessionRef}
          cancel={cancelSession}
        />
        <TerminateQueryModal
          ref={this.terminateQueryRef}
          cancel={cancelQuery}
        />
      </div>
    );
  }

  onCachedTransactionFingerprintClick(fingerprintDec: string): void {
    const session = this.props.session?.session;
    if (session == null) return;

    const now = moment.utc();
    const end = session.end ? TimestampToMoment(session.end) : now;

    // Round to the next hour if it is not in the future.
    const roundToNextHour =
      end.clone().endOf("hour").isBefore(now) &&
      (end.minute() || end.second() || end.millisecond());

    if (roundToNextHour) {
      end.add(1, "hour").startOf("hour");
    }

    const start = TimestampToMoment(session.start).startOf("hour");
    const range = { start, end };
    const timeScale = createTimeScaleFromDateRange(range);
    this.props.setTimeScale(timeScale);
    this.props.history.push(`/transaction/${fingerprintDec}`);
  }

  renderContent = (): React.ReactElement => {
    if (!this.props.session) {
      return null;
    }
    const { isTenant } = this.props;
    const { session } = this.props.session;

    if (!session) {
      return (
        <section className={cx("section")}>
          <h3>Unable to find session</h3>
          There is no session with the id{" "}
          {getMatchParamByName(this.props.match, sessionAttr)}.
          <br />
          {`The session’s details may no longer be available because they were
          removed from cache, which is controlled by the cluster settings
          'sql.closed_session_cache.capacity' and
          'sql.closed_session_cache.time_to_live'.`}
        </section>
      );
    }

    let txnInfo = (
      <SummaryCard className={cx("details-section")}>
        No Active Transaction
      </SummaryCard>
    );
    if (session.active_txn && session.end == null) {
      const txn = session.active_txn;
      const start = TimestampToMoment(txn.start);
      txnInfo = (
        <>
          <Row gutter={24}>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label={"Transaction Start Time"}
                  value={<Timestamp time={start} format={DATE_FORMAT_24_TZ} />}
                />
                <SummaryCardItem
                  label={"Number of Statements Executed"}
                  value={txn.num_statements_executed}
                />
                <SummaryCardItem
                  label={"Number of Retries"}
                  value={txn.num_retries}
                />
                <SummaryCardItem
                  label={"Number of Automatic Retries"}
                  value={txn.num_auto_retries}
                />
              </SummaryCard>
            </Col>
            <Col className="gutter-row" span={12}>
              <SummaryCard className={cx("summary-card")}>
                <SummaryCardItem
                  label={"Read Only"}
                  value={yesOrNo(txn.read_only)}
                />
                <SummaryCardItem
                  label={"AS OF SYSTEM TIME?"}
                  value={yesOrNo(txn.is_historical)}
                />
                <SummaryCardItem label={"Priority"} value={txn.priority} />
                <MemoryUsageItem
                  allocBytes={txn.alloc_bytes}
                  maxAllocBytes={txn.max_alloc_bytes}
                />
              </SummaryCard>
            </Col>
          </Row>
        </>
      );
    }

    let curStmtInfo = session.last_active_query ? (
      <SqlBox value={session.last_active_query} size={SqlBoxSize.CUSTOM} />
    ) : (
      <SummaryCard className={cx("details-section")}>
        No Active Statement
      </SummaryCard>
    );

    if (session.active_queries?.length > 0) {
      const stmt = session.active_queries[0];
      curStmtInfo = (
        <React.Fragment>
          <SqlBox value={stmt.sql} size={SqlBoxSize.CUSTOM} />
          <SummaryCard className={cx("details-section")}>
            <Row>
              <Col className="gutter-row" span={10}>
                <SummaryCardItem
                  label={"Execution Start Time"}
                  value={
                    <Timestamp
                      time={TimestampToMoment(stmt.start)}
                      format={DATE_FORMAT_24_TZ}
                    />
                  }
                  className={cx("details-item")}
                />
              </Col>
              <Col className="gutter-row" span={4} />
              <Col className="gutter-row" span={10}>
                <SummaryCardItem
                  label={"Distributed Execution?"}
                  value={yesOrNo(stmt.is_distributed)}
                  className={cx("details-item")}
                />
              </Col>
            </Row>
          </SummaryCard>
        </React.Fragment>
      );
    }

    return (
      <>
        <Row gutter={24}>
          <Col className="gutter-row" span={12}>
            <SummaryCard className={cx("summary-card")}>
              <SummaryCardItem
                label="Session Start Time"
                value={
                  <Timestamp
                    time={TimestampToMoment(session.start)}
                    format={DATE_FORMAT_24_TZ}
                  />
                }
              />
              {session.end && (
                <SummaryCardItem
                  label={"Session End Time"}
                  value={
                    <Timestamp
                      time={TimestampToMoment(session.end)}
                      format={DATE_FORMAT_24_TZ}
                    />
                  }
                />
              )}
              <SummaryCardItem
                label={"Session Active Duration"}
                value={DurationToMomentDuration(
                  session.total_active_time,
                ).humanize()}
              />
              {!isTenant && (
                <SummaryCardItem
                  label={"Gateway Node"}
                  value={
                    this.props.uiConfig?.showGatewayNodeLink ? (
                      <div className={cx("session-details-link")}>
                        <NodeLink
                          nodeId={session.node_id.toString()}
                          nodeNames={this.props.nodeNames}
                        />
                      </div>
                    ) : (
                      session.node_id.toString()
                    )
                  }
                />
              )}
              <SummaryCardItem
                label={"Application Name"}
                value={session.application_name}
              />
              <SummaryCardItem
                label={"Status"}
                value={
                  <div>
                    <CircleFilled
                      className={cx(getStatusClassname(session.status))}
                    />
                    <span>{getStatusString(session.status)}</span>
                  </div>
                }
              />
            </SummaryCard>
          </Col>
          <Col className="gutter-row" span={12}>
            <SummaryCard className={cx("summary-card")}>
              <SummaryCardItem
                label={"Client IP Address"}
                value={session.client_address}
              />
              <MemoryUsageItem
                allocBytes={session.alloc_bytes}
                maxAllocBytes={session.max_alloc_bytes}
              />
              <SummaryCardItem label={"User Name"} value={session.username} />
              <SummaryCardItem
                label="Transaction Count"
                value={Count(session.num_txns_executed)}
              />
            </SummaryCard>
          </Col>
        </Row>
        <Text textType={TextTypes.Heading5} className={cx("details-header")}>
          Transaction
        </Text>
        {txnInfo}
        <Text textType={TextTypes.Heading5} className={cx("details-header")}>
          Most Recent Statement
        </Text>
        {curStmtInfo}
        <div>
          <Text textType={TextTypes.Heading5} className={cx("details-header")}>
            Most Recent Transaction Fingerprints Executed
          </Text>
          <Text textType={TextTypes.Caption}>
            A list of the most recent transaction fingerprint IDs executed by
            this session represented in hexadecimal.
          </Text>
          <SummaryCard
            className={cx("details-section", "session-txn-fingerprints")}
          >
            {session.txn_fingerprint_ids.map((txnFingerprintID, i) => (
              <Button
                type="unstyled-link"
                className={cx("link-txn-fingerprint-id")}
                onClick={() =>
                  this.onCachedTransactionFingerprintClick(
                    txnFingerprintID.toString(10),
                  )
                }
                key={i}
              >
                {txnFingerprintID.toString(16)}
              </Button>
            ))}
          </SummaryCard>
        </div>
      </>
    );
  };
}

export default SessionDetails;
