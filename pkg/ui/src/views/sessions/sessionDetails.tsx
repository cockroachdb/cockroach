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
import {getMatchParamByName} from "src/util/query";
import {sessionAttr} from "src/util/constants";
import {Helmet} from "react-helmet";
import Loading from "src/views/shared/components/loading";
import _ from "lodash";
import {Link, RouteComponentProps, withRouter} from "react-router-dom";
import {createSelector} from "reselect";
import {Pick} from "src/util/pick";
import {AdminUIState} from "src/redux/state";
import {SessionsResponseMessage} from "src/util/api";
import {connect} from "react-redux";
import {CachedDataReducerState, refreshSessions} from "src/redux/apiReducers";
import {byteArrayToUuid, SessionInfo} from "src/views/sessions/sessionsTable";
import styles from "./sessionDetails.module.styl";
import classNames from "classnames/bind";
import {SummaryCard, SummaryCardItem} from "src/views/shared/components/summaryCard";
import SqlBox from "src/views/shared/components/sql/box";
import {TimestampToMoment} from "src/util/convert";
import {Bytes, DATE_FORMAT} from "src/util/format";
import {Col, Row} from "antd";
import {nodeDisplayNameByIDSelector} from "src/redux/nodes";
import {NodeLink, StatementLinkTarget} from "src/views/statements/statementsTableContent";
import TerminateSessionModal, {TerminateSessionModalRef} from "src/views/sessions/terminateSessionModal";
import TerminateQueryModal, {TerminateQueryModalRef} from "src/views/sessions/terminateQueryModal";
import { Button } from "@cockroachlabs/admin-ui-components";
import { ArrowLeft } from "@cockroachlabs/icons";
import { Text, TextTypes } from "src/components";

interface OwnProps {
  id: string;
  nodeNames: { [nodeId: string]: string };
  session: SessionInfo;
  sessionError: Error | null;
  refreshSessions: typeof refreshSessions;
}

const cx = classNames.bind(styles);
export type SessionDetailsProps = OwnProps & RouteComponentProps;

function yesOrNo(b: boolean) {
  return b ? "Yes" : "No";
}

export const MemoryUsageItem: React.FC<{
  alloc_bytes: Long
  max_alloc_bytes: Long,
}> = ( {alloc_bytes, max_alloc_bytes}) => (
  <SummaryCardItem
    label={"Memory Usage"}
    value={
      Bytes(alloc_bytes?.toNumber()) + "/" + Bytes(max_alloc_bytes?.toNumber())
    }
    className={cx("details-item")}
  />
 );

export class SessionDetails extends React.Component<SessionDetailsProps, {}> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;

  componentDidMount() {
    this.props.refreshSessions();
  }

  componentDidUpdate() {
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

  backToSessionsPage = () => {
    const { history, location } = this.props;
    history.push({
      ...location,
      pathname: "/sessions",
    });
  }

  render() {
    const sessionID = getMatchParamByName(this.props.match, sessionAttr);
    const { sessionError } = this.props;
    const session = this.props.session?.session;
    const showActionButtons = !!session && !sessionError;
    return (
      <div>
        <Helmet title={`Details | ${sessionID} | Sessions`} />
        <div className={cx("section", "page--header")}>
          <Button
            onClick={this.backToSessionsPage}
            type="unstyled-link"
            size="small"
            icon={<ArrowLeft fontSize={"10px"} />}
            iconPosition="left"
          >
            Sessions
          </Button>
          <div className={cx("heading-with-controls")}>
            <h1 className={cx("base-heading", "page--header__title")}>
              Session details
            </h1>
            {
              showActionButtons && (
                <div className={cx("heading-controls-group")}>
                  <Button
                    disabled={session.active_queries?.length === 0}
                    onClick={() => {
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
                    onClick={() => this.terminateSessionRef?.current?.showModalFor({
                      session_id: session.id,
                      node_id: session.node_id.toString(),
                    })}
                    type="secondary"
                    size="small"
                  >
                    Cancel session
                  </Button>
                </div>
              )
            }
          </div>
        </div>
        <section className={cx("section", "section--container")}>
          <Loading
            loading={_.isNil(this.props.session)}
            error={this.props.sessionError}
            render={this.renderContent}
          />
        </section>
        <TerminateSessionModal ref={this.terminateSessionRef} />
        <TerminateQueryModal ref={this.terminateQueryRef} />
      </div>
    );
  }

  renderContent = () => {
    if (!this.props.session) {
      return null;
    }
    const { session } = this.props.session;

    if (!session) {
      return (
        <section className={cx("section")}>
          <h3>Unable to find session</h3>
          There is no currently active session with the id {getMatchParamByName(this.props.match, sessionAttr)}.
          <div>
            <Link className={cx("back-link")} to={ "/sessions" }>
              Back to Sessions
            </Link>
          </div>
        </section>
      );
    }

    let txnInfo = (
      <React.Fragment>
        No Active Transaction
      </React.Fragment>
    );
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
          <Col className="gutter-row" span={4}>
          </Col>
          <Col className="gutter-row" span={10}>
            <SummaryCardItem
              label={"Priority"}
              value={txn.priority}
              className={cx("details-item")}
            />
            <SummaryCardItem
              label={"Read Only?"}
              value={yesOrNo(txn.read_only)}
              className={cx("details-item")}
            />
            <SummaryCardItem
              label={"AS OF SYSTEM TIME?"}
              value={yesOrNo(txn.is_historical)}
              className={cx("details-item")}
            />
            <MemoryUsageItem alloc_bytes={txn.alloc_bytes} max_alloc_bytes={txn.max_alloc_bytes}/>
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
        <SqlBox value={stmt.sql}>
        </SqlBox>
        <SummaryCard className={cx("details-section")}>
          <Row>
            <Col className="gutter-row" span={10}>
              <SummaryCardItem
                label={"Execution Start Time"}
                value={TimestampToMoment(stmt.start).format(DATE_FORMAT)}
                className={cx("details-item")}
              />
              <Link to={ StatementLinkTarget({
                statement: stmt.sql,
                anonStatement: stmt.sql_anon,
                implicitTxn: session.active_txn?.implicit,
                app: "",
                search: "",
              })}>
                View Statement Details
              </Link>
            </Col>
            <Col className="gutter-row" span={4}>
            </Col>
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
                value={ TimestampToMoment(session.start).format(DATE_FORMAT)}
                className={cx("details-item")}
              />
              <SummaryCardItem
                label={"Gateway Node"}
                value={
                  <NodeLink nodeId={session.node_id.toString()} nodeNames={this.props.nodeNames}/>
                }
                className={cx("details-item")}
              />
            </Col>
            <Col className="gutter-row" span={4}>
            </Col>
            <Col className="gutter-row" span={10}>
              <SummaryCardItem
                label={"Client Address"}
                value={session.client_address}
                className={cx("details-item")}
              />
              <MemoryUsageItem alloc_bytes={session.alloc_bytes} max_alloc_bytes={session.max_alloc_bytes}/>
            </Col>
          </Row>
        </SummaryCard>
        <Text textType={TextTypes.Heading5} className={cx("details-header")}>
          Transaction
        </Text>
        <SummaryCard className={cx("details-section")}>
          {txnInfo}
        </SummaryCard>
        <Text textType={TextTypes.Heading5} className={cx("details-header")}>
          Statement
        </Text>
        {curStmtInfo}
      </React.Fragment>
    );
  }
}

type SessionsState = Pick<AdminUIState, "cachedData", "sessions">;

export const selectSession = createSelector(
  (state: SessionsState) => state.cachedData.sessions,
  (_state: SessionsState, props: RouteComponentProps) => props,
  (
    state: CachedDataReducerState<SessionsResponseMessage>,
    props: RouteComponentProps<any>,
  ) => {
    if (!state.data) {
      return null;
    }
    const sessionID = getMatchParamByName(props.match, sessionAttr);
    return {session: state.data.sessions.find(
      session => byteArrayToUuid(session.id) === sessionID)};
  },
);
// tslint:disable-next-line:variable-name
const SessionDetailsPageConnected = withRouter(connect(
  (state: AdminUIState, props: RouteComponentProps) => ({
    nodeNames: nodeDisplayNameByIDSelector(state),
    session: selectSession(state, props),
    sessionError: state.cachedData.sessions.lastError,
  }),
  {
    refreshSessions,
  },
)(SessionDetails));

export default SessionDetailsPageConnected;
