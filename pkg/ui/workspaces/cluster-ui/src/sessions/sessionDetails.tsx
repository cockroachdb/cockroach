// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { getMatchParamByName } from "src/util/query";
import { sessionAttr } from "src/util/constants";
import { Helmet } from "react-helmet";
import { Loading } from "../loading";
import _ from "lodash";
import { Link, RouteComponentProps } from "react-router-dom";

import { SessionInfo } from "./sessionsTable";

import { SummaryCard, SummaryCardItem } from "../summaryCard";
import SQLActivityError from "../sqlActivity/errorComponent";

import { TimestampToMoment } from "src/util/convert";
import { Bytes, DATE_FORMAT } from "src/util/format";
import { Col, Row } from "antd";

import TerminateSessionModal, {
  TerminateSessionModalRef,
} from "./terminateSessionModal";
import TerminateQueryModal, {
  TerminateQueryModalRef,
} from "./terminateQueryModal";
import { Button } from "../button";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Text, TextTypes } from "../text";
import { SqlBox } from "src/sql/box";
import {
  NodeLink,
  StatementLinkTarget,
} from "src/statementsTable/statementsTableContent";

import {
  ICancelQueryRequest,
  ICancelSessionRequest,
} from "src/store/terminateQuery";
import { UIConfigState } from "src/store";

import statementsPageStyles from "src/statementsPage/statementsPage.module.scss";
import styles from "./sessionDetails.module.scss";
import classNames from "classnames/bind";
import { commonStyles } from "src/common";
import { CircleFilled } from "../icon";

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
}

export type SessionDetailsProps = OwnProps & RouteComponentProps;

function yesOrNo(b: boolean) {
  return b ? "Yes" : "No";
}

export const MemoryUsageItem: React.FC<{
  alloc_bytes: Long;
  max_alloc_bytes: Long;
}> = ({ alloc_bytes, max_alloc_bytes }) => (
  <SummaryCardItem
    label={"Memory Usage"}
    value={
      Bytes(alloc_bytes?.toNumber()) + "/" + Bytes(max_alloc_bytes?.toNumber())
    }
    className={cx("details-item")}
  />
);

export class SessionDetails extends React.Component<SessionDetailsProps> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;
  static defaultProps = {
    uiConfig: { showGatewayNodeLink: true },
    isTenant: false,
  };

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
              Session details
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
                  Cancel query
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
                  Cancel session
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
            loading={_.isNil(this.props.session)}
            page={"sessions details"}
            error={this.props.sessionError}
            render={this.renderContent}
            renderError={() =>
              SQLActivityError({
                statsType: "sessions",
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
          There is no currently active session with the id{" "}
          {getMatchParamByName(this.props.match, sessionAttr)}.
          <div>
            <Link className={cx("back-link")} to={"/sessions"}>
              Back to Sessions
            </Link>
          </div>
        </section>
      );
    }

    let txnInfo = <React.Fragment>No Active Transaction</React.Fragment>;
    if (session.active_txn) {
      const txn = session.active_txn;
      const start = TimestampToMoment(txn.start);
      txnInfo = (
        <Row gutter={16}>
          <Col className="gutter-row" span={10}>
            <SummaryCardItem
              label={"Transaction Start Time"}
              value={start.format(DATE_FORMAT)}
              className={cx("details-item")}
            />
            <SummaryCardItem
              label={"Number of Statements Executed"}
              value={txn.num_statements_executed}
              className={cx("details-item")}
            />
            <SummaryCardItem
              label={"Number of Retries"}
              value={txn.num_retries}
              className={cx("details-item")}
            />
            <SummaryCardItem
              label={"Number of Automatic Retries"}
              value={txn.num_auto_retries}
              className={cx("details-item")}
            />
          </Col>
          <Col className="gutter-row" span={4} />
          <Col className="gutter-row" span={10}>
            <SummaryCardItem
              label={"Read Only"}
              value={yesOrNo(txn.read_only)}
              className={cx("details-item")}
            />
            <SummaryCardItem
              label={"AS OF SYSTEM TIME?"}
              value={yesOrNo(txn.is_historical)}
              className={cx("details-item")}
            />
            <SummaryCardItem
              label={"Priority"}
              value={txn.priority}
              className={cx("details-item")}
            />
            <MemoryUsageItem
              alloc_bytes={txn.alloc_bytes}
              max_alloc_bytes={txn.max_alloc_bytes}
            />
          </Col>
        </Row>
      );
    }
    let curStmtInfo = (
      <SummaryCard className={cx("details-section")}>
        No Active Statement
      </SummaryCard>
    );

    if (session.active_queries?.length > 0) {
      const stmt = session.active_queries[0];
      curStmtInfo = (
        <React.Fragment>
          <SqlBox value={stmt.sql} />
          <SummaryCard className={cx("details-section")}>
            <Row>
              <Col className="gutter-row" span={10}>
                <SummaryCardItem
                  label={"Execution Start Time"}
                  value={TimestampToMoment(stmt.start).format(DATE_FORMAT)}
                  className={cx("details-item")}
                />
                <SummaryCardItem
                  label={
                    <div className={cx("session-details-link")}>
                      <Link
                        to={StatementLinkTarget({
                          statementFingerprintID: stmt.id,
                          statementNoConstants: stmt.sql_no_constants,
                          implicitTxn: session.active_txn?.implicit,
                          app: "",
                        })}
                        onClick={() =>
                          this.props.onStatementClick &&
                          this.props.onStatementClick()
                        }
                      >
                        View Statement Details
                      </Link>
                    </div>
                  }
                  value={""}
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
      <React.Fragment>
        <SummaryCard className={cx("details-section")}>
          <Row gutter={12}>
            <Col className="gutter-row" span={10}>
              <SummaryCardItem
                label={"Session Start Time"}
                value={TimestampToMoment(session.start).format(DATE_FORMAT)}
                className={cx("details-item")}
              />
              {!isTenant && (
                <SummaryCardItem
                  label={"Gateway Node"}
                  value={
                    this.props.uiConfig.showGatewayNodeLink ? (
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
                  className={cx("details-item")}
                />
              )}
              <SummaryCardItem
                label={"Application name"}
                value={session.application_name}
                className={cx("details-item")}
              />
              <SummaryCardItem
                label={"Status"}
                value={
                  <div>
                    <CircleFilled
                      className={cx(
                        session.active_queries.length > 0
                          ? "session-status-icon__active"
                          : "session-status-icon__idle",
                      )}
                    />
                    <span>
                      {session.active_queries.length > 0 ? "Active" : "Idle"}
                    </span>
                  </div>
                }
                className={cx("details-item")}
              />
            </Col>
            <Col className="gutter-row" span={4} />
            <Col className="gutter-row" span={10}>
              <SummaryCardItem
                label={"Client IP Address"}
                value={session.client_address}
                className={cx("details-item")}
              />
              <MemoryUsageItem
                alloc_bytes={session.alloc_bytes}
                max_alloc_bytes={session.max_alloc_bytes}
              />
              <SummaryCardItem
                label={"User name"}
                value={session.username}
                className={cx("details-item")}
              />
            </Col>
          </Row>
        </SummaryCard>
        <Text textType={TextTypes.Heading5} className={cx("details-header")}>
          Transaction
        </Text>
        <SummaryCard className={cx("details-section")}>{txnInfo}</SummaryCard>
        <Text textType={TextTypes.Heading5} className={cx("details-header")}>
          Most Recent Statement
        </Text>
        {curStmtInfo}
      </React.Fragment>
    );
  };
}

export default SessionDetails;
