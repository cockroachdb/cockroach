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
import { forIn, isNil, merge } from "lodash";

import { getMatchParamByName } from "src/util/query";
import { appAttr } from "src/util/constants";
import {
  makeSessionsColumns,
  SessionInfo,
  SessionsSortedTable,
} from "./sessionsTable";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import classNames from "classnames/bind";
import { sessionsTable } from "src/util/docs";

import emptyTableResultsIcon from "../assets/emptyState/empty-table-results.svg";

import { Pagination } from "antd";
import { SortSetting } from "../sortedtable";
import {
  ISortedTablePagination,
  ResultsPerPageLabel,
  EmptyTable,
  Anchor,
  Loading,
} from "src";
import TerminateQueryModal, {
  TerminateQueryModalRef,
} from "./terminateQueryModal";
import TerminateSessionModal, {
  TerminateSessionModalRef,
} from "./terminateSessionModal";

import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";

const sortableTableCx = classNames.bind(sortableTableStyles);

type ICancelQueryRequest = any;
interface OwnProps {
  sessions: SessionInfo[];
  sessionsError: Error | null;
  //refreshSessions: typeof refreshSessions;
  refreshSessions: any;
  cancel?: (req: ICancelQueryRequest) => void;
  onPageChanged?: (newPage: number) => void;
}

export interface SessionsPageState {
  sortSetting: SortSetting;
  pagination: ISortedTablePagination;
}

export type SessionsPageProps = OwnProps & RouteComponentProps<any>;

export class SessionsPage extends React.Component<
  SessionsPageProps,
  SessionsPageState
> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;

  constructor(props: SessionsPageProps) {
    super(props);
    const defaultState = {
      sortSetting: {
        sortKey: 2, // Sort by Statement Age column as default option.
        ascending: false,
      },
      pagination: {
        pageSize: 20,
        current: 1,
      },
    };

    const stateFromHistory = this.getStateFromHistory();
    this.state = merge(defaultState, stateFromHistory);
    this.terminateSessionRef = React.createRef();
    this.terminateQueryRef = React.createRef();
  }

  getStateFromHistory = (): Partial<SessionsPageState> => {
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const sortKey = searchParams.get("sortKey") || undefined;
    const ascending = searchParams.get("ascending") || undefined;

    return {
      sortSetting: {
        sortKey: sortKey,
        ascending: Boolean(ascending),
      },
    };
  };

  syncHistory = (params: Record<string, string | undefined>) => {
    const { history } = this.props;
    const currentSearchParams = new URLSearchParams(history.location.search);
    forIn(params, (value, key) => {
      if (!value) {
        currentSearchParams.delete(key);
      } else {
        currentSearchParams.set(key, value);
      }
    });

    history.location.search = currentSearchParams.toString();
    history.replace(history.location);
  };

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });

    this.syncHistory({
      sortKey: ss.sortKey,
      ascending: Boolean(ss.ascending).toString(),
    });
  };

  resetPagination = () => {
    this.setState(prevState => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  };

  componentDidMount() {
    this.props.refreshSessions();
  }

  componentDidUpdate = (__: SessionsPageProps, _: SessionsPageState) => {
    this.props.refreshSessions();
  };

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
    this.props.onPageChanged(current);
  };

  renderSessions = () => {
    const sessionsData = this.props.sessions;
    const { pagination } = this.state;
    return (
      <div>
        <section className={sortableTableCx("cl-table-container")}>
          <div>
            <h4>
              <ResultsPerPageLabel
                pagination={{
                  ...pagination,
                  total: this.props.sessions.length,
                }}
                pageName={"active sessions"}
                selectedApp={appAttr}
              />
            </h4>
          </div>
          <SessionsSortedTable
            className="sessions-table"
            data={sessionsData}
            columns={makeSessionsColumns(
              this.terminateSessionRef,
              this.terminateQueryRef,
            )}
            renderNoResult={
              <EmptyTable
                title="No sessions are currently running"
                icon={emptyTableResultsIcon}
                message="Sessions show you which statements and transactions are running for the active session."
                footer={
                  <Anchor href={sessionsTable} target="_blank">
                    Learn more about sessions
                  </Anchor>
                }
              />
            }
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
            pagination={pagination}
          />
        </section>
        <Pagination
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={sessionsData.length}
          onChange={this.onChangePage}
        />
      </div>
    );
  };

  render() {
    const { match, cancel } = this.props;
    const app = getMatchParamByName(match, appAttr);
    return (
      <React.Fragment>
        <Helmet title={app ? `${app} App | Sessions` : "Sessions"} />

        <section>
          <h1>Sessions</h1>
        </section>

        <Loading
          loading={isNil(this.props.sessions)}
          error={this.props.sessionsError}
          render={this.renderSessions}
        />
        <TerminateSessionModal ref={this.terminateSessionRef} cancel={cancel} />
        <TerminateQueryModal ref={this.terminateQueryRef} cancel={cancel} />
      </React.Fragment>
    );
  }
}

export default SessionsPage;
