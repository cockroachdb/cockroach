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
import {forIn, isNil, merge} from "lodash";
import {SortSetting} from "src/views/shared/components/sortabletable";
import {getMatchParamByName} from "src/util/query";
import {appAttr} from "src/util/constants";
import {makeSessionsColumns, SessionInfo, SessionsSortedTable} from "src/views/sessions/sessionsTable";
import Helmet from "react-helmet";
import Loading from "src/views/shared/components/loading";
import {Pick} from "src/util/pick";
import {RouteComponentProps, withRouter} from "react-router-dom";
import {connect} from "react-redux";
import {AdminUIState} from "src/redux/state";
import {CachedDataReducerState, refreshSessions} from "src/redux/apiReducers";

import sortableTableStyles from "src/views/shared/components/sortabletable/sortabletable.module.styl";
import classNames from "classnames/bind";
import styles from "src/views/sessions/sessionsPage.module.styl";
import {paginationPageCount} from "src/components/pagination/pagination";
import {ISortedTablePagination} from "src/views/shared/components/sortedtable";
import {createSelector} from "reselect";
import {SessionsResponseMessage} from "src/util/api";
import TerminateSessionModal, {TerminateSessionModalRef} from "src/views/sessions/terminateSessionModal";
import TerminateQueryModal, {TerminateQueryModalRef} from "src/views/sessions/terminateQueryModal";
import Pagination from "antd/lib/pagination";
import Icon from "antd/lib/icon";
import {showSessions} from "src/util/docs";

const sortableTableCx = classNames.bind(sortableTableStyles);
const cx = classNames.bind(styles);

interface OwnProps {
  sessions: SessionInfo[];
  sessionsError: Error | null;
  refreshSessions: typeof refreshSessions;
  onPageChanged?: (newPage: number) => void;
}

export interface SessionsPageState {
  sortSetting: SortSetting;
  pagination: ISortedTablePagination;
}

export type SessionsPageProps = OwnProps & RouteComponentProps<any>;

export class SessionsPage extends React.Component<SessionsPageProps, SessionsPageState> {
  terminateSessionRef: React.RefObject<TerminateSessionModalRef>;
  terminateQueryRef: React.RefObject<TerminateQueryModalRef>;

  constructor(props: SessionsPageProps) {
    super(props);
    const defaultState = {
      sortSetting: {
        sortKey: 2, // Sort by Query Age column as default option.
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
  }

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
  }

  changeSortSetting = (ss: SortSetting) => {
    this.setState({
      sortSetting: ss,
    });

    this.syncHistory({
      "sortKey": ss.sortKey,
      "ascending": Boolean(ss.ascending).toString(),
    });
  }

  resetPagination = () => {
    this.setState((prevState) => {
      return {
        pagination: {
          current: 1,
          pageSize: prevState.pagination.pageSize,
        },
      };
    });
  }

  componentDidMount() {
    this.props.refreshSessions();
  }

  componentDidUpdate = (__: SessionsPageProps, _: SessionsPageState) => {
    this.props.refreshSessions();
  }

  onChangePage = (current: number) => {
    const { pagination } = this.state;
    this.setState({ pagination: { ...pagination, current } });
    this.props.onPageChanged(current);
  }

  renderPage = (
    _page: number,
    type: "page" | "prev" | "next" | "jump-prev" | "jump-next",
    originalElement: React.ReactNode,
  ) => {
    switch (type) {
      case "jump-prev":
        return (
          <div className={cx("_pg-jump")}>
            <Icon type="left" />
            <span className={cx("_jump-dots")}>•••</span>
          </div>
        );
      case "jump-next":
        return (
          <div className={cx("_pg-jump")}>
            <Icon type="right" />
            <span className={cx("_jump-dots")}>•••</span>
          </div>
        );
      default:
        return originalElement;
    }
  }

  renderSessions = () => {
    const sessionsData = this.props.sessions;
    const { pagination } = this.state;
    return (
      <div>
        <section className={sortableTableCx("cl-table-container")}>
         <div className={cx("cl-table-statistic")}>
            <h4 className={cx("cl-count-title")}>
              {
                paginationPageCount(
                { ...pagination, total: this.props.sessions.length },
                "active sessions", null, appAttr, null)
              }
            </h4>
          </div>
          <SessionsSortedTable
            className="sessions-table"
            data={sessionsData}
            columns={
              makeSessionsColumns(this.terminateSessionRef, this.terminateQueryRef)
            }
            empty={sessionsData.length === 0}
            emptyProps={{
              title: "There are no currently running sessions.",
              description: "Sessions shows you what statements and transactions the currently active sessions are running.",
              label: "Learn more",
              // TODO(jordan): point this to a more appropriate page.
              buttonHref: showSessions,
            }}
            sortSetting={this.state.sortSetting}
            onChangeSortSetting={this.changeSortSetting}
            pagination={pagination}
          />
        </section>
        <Pagination
          size="small"
          itemRender={
            this.renderPage as (
              page: number,
              type: "page" | "prev" | "next" | "jump-prev" | "jump-next",
            ) => React.ReactNode
          }
          pageSize={pagination.pageSize}
          current={pagination.current}
          total={sessionsData.length}
          onChange={this.onChangePage}
          hideOnSinglePage
        />
      </div>
    );
  }

  render() {
    const { match } = this.props;
    const app = getMatchParamByName(match, appAttr);
    return (
      <React.Fragment>
        <Helmet title={app ? `${app} App | Active Sessions` : "Active Sessions"} />

        <section className={cx("section")}>
          <h1 className={cx("base-heading")}>Active Sessions</h1>
        </section>

        <Loading
          loading={isNil(this.props.sessions)}
          error={this.props.sessionsError}
          render={this.renderSessions}
        />
        <TerminateSessionModal ref={this.terminateSessionRef} />
        <TerminateQueryModal ref={this.terminateQueryRef} />
      </React.Fragment>
    );
  }
}

type SessionsState = Pick<AdminUIState, "cachedData", "sessions">;

export const selectSessions = createSelector(
  (state: SessionsState) => state.cachedData.sessions,
  (_state: SessionsState, props: RouteComponentProps) => props,
  (
      state: CachedDataReducerState<SessionsResponseMessage>,
      _: RouteComponentProps<any>,
  ) => {
    if (!state.data) {
      return null;
    }
    return state.data.sessions.map(session => {
      return {session};
    });
  },
);

// tslint:disable-next-line:variable-name
const SessionsPageConnected = withRouter(connect(
  (state: AdminUIState, props: RouteComponentProps) => ({
    sessions: selectSessions(state, props),
    sessionsError: state.cachedData.sessions.lastError,
  }),
  {
    refreshSessions,
  },
)(SessionsPage));

export default SessionsPageConnected;
