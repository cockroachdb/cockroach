import d3 from "d3";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouterState } from "react-router";
import { createSelector } from "reselect";

import { refreshQueries } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { statementAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import { SqlBox } from "src/views/shared/components/sql/box";
import { SummaryBar, SummaryHeadlineStat } from "src/views/shared/components/summaryBar";

interface StatementDetailsOwnProps {
  queries: CachedDataReducerState<QueriesResponseMessage>;
  refreshQueries: typeof refreshQueries;
}

type StatementDetailsProps = StatementDetailsOwnProps & RouterState;

class StatementDetails extends React.Component<StatementDetailsProps> {
  componentWillMount() {
    this.props.refreshQueries();
  }

  componentWillReceiveProps() {
    this.props.refreshQueries();
  }

  render() {
    if (!this.props.statement) {
      return "loading...";
    }

    const { stats } = this.props.statement;

    const count = FixLong(stats.count).toInt();
    const firstAttemptCount = FixLong(stats.first_attempt_count).toInt();

    return (
      <div>
        <Helmet>
          <title>{`Details | Statements`}</title>
        </Helmet>
        <section className="section">
          <h2>Statement Details</h2>
          <div className="content l-columns">
            <div className="l-columns__left">
              <SqlBox value={ this.props.statement.key.query } />
              <pre>{JSON.stringify(this.props.statement.stats, null, 2)}</pre>
            </div>
            <div className="l-columns__right">
              <SummaryBar>
                <SummaryHeadlineStat
                  title="Execution Count"
                  tooltip="Number of times this statement has executed."
                  value={ count } />
                <SummaryHeadlineStat
                  title="Executed without Retry"
                  tooltip="Portion of executions free of retries."
                  value={ firstAttemptCount / count }
                  format={ d3.format("%") } />
                <SummaryHeadlineStat
                  title="Mean Number of Rows"
                  tooltip="The average number of rows returned or affected."
                  value={ Math.round(stats.num_rows.mean) } />
              </SummaryBar>
            </div>
          </div>
        </section>
      </div>
    );
  }
}

const selectStatement = createSelector(
  (state: AdminUIState) => state.cachedData.queries.data && state.cachedData.queries.data.queries,
  (_state: AdminUIState, props: RouterState) => props.params[statementAttr],
  (haystack, needle) => haystack && _.find(haystack, hay => hay.key.query === needle),
);

// tslint:disable-next-line:variable-name
const StatementDetailsConnected = connect(
  (state: AdminUIState, props: RouterState) => ({
    statement: selectStatement(state, props),
  }),
  {
    refreshQueries,
  },
)(StatementDetails);

export default StatementDetailsConnected;
