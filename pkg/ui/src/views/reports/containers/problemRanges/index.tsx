import React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { refreshProblemRanges } from "src/redux/apiReducers";
import { RouterState } from "react-router";

interface ProblemRangesOwnProps {
  problemRanges: protos.cockroach.server.serverpb.ProblemRangesResponse;
  refreshProblemRanges: typeof refreshProblemRanges;
}

type ProblemRangesProps = ProblemRangesOwnProps & RouterState;

/**
 * Renders the Problem Ranges page.
 */
class ProblemRanges extends React.Component<ProblemRangesProps, {}> {
  refresh(props = this.props) {
    props.refreshProblemRanges(new protos.cockroach.server.serverpb.ProblemRangesRequest({
      node_id: (!_.isEmpty(props.location.query.node_id)) ? props.location.query.node_id : "",
    }));
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentWillReceiveProps(nextProps: ProblemRangesProps) {
    if (this.props.location !== nextProps.location) {
      this.refresh(nextProps);
    }
  }

  render() {
    const { problemRanges } = this.props;
    if (!problemRanges) {
      return (
        <div className="section">
          <h1>Loading cluster status...</h1>
        </div>
      );
    }

    const titleText = (problemRanges.node_id !== 0) ?
      `Problem Ranges for Node n${problemRanges.node_id}` : `Problem Ranges for the Cluster`;

    let failures: JSX.Element;
    if (problemRanges.failures.length > 0) {
      failures = (
        <div>
          <h2>Failures</h2>
          <div className="failure-table">
            <div className="failure-table__row failure-table__row--header">
              <div className="failure-table__cell failure-table__cell--short">Node</div>
              <div className="failure-table__cell">Error</div>
            </div>
            {
              _.map(problemRanges.failures, (failure) => (
                <div className="failure-table__row" key={failure.node_id}>
                  <div className="failure-table__cell failure-table__cell--short">n{failure.node_id}</div>
                  <div className="failure-table__cell">title={failure.error_message}>{failure.error_message}</div>
                </div>
              ))
            }
          </div>
        </div>
      );
    }

    const problemTable = (name: string, ids: Long[]) => {
      if (!ids || ids.length === 0) {
        return null;
      }
      return (
        <div>
          <h2>{name}</h2>
          <div className="problems-table">
            <div className="problems-table__row">
              <div className="problems-table__cell">
                {
                  _.map(ids, (id) => (
                    <span key={id.toNumber()}>
                      <a href={`/debug/range?id=${id}`}>{id.toNumber()}</a>
                      <span> </span>
                    </span>
                  ))
                }
              </div>
            </div>
          </div>
        </div>
      );
    };

    return (
      <div className="section">
        <h1>{titleText}</h1>
        {failures}
        {(_.isEmpty(problemRanges.unavailable_range_ids) &&
          _.isEmpty(problemRanges.no_raft_leader_range_ids) &&
          _.isEmpty(problemRanges.no_lease_range_ids) &&
          _.isEmpty(problemRanges.raft_leader_not_lease_holder_range_ids) &&
          _.isEmpty(problemRanges.underreplicated_range_ids)) ?
          <h2>No problems!</h2> :
          <div>
            {problemTable("Unavailable", problemRanges.unavailable_range_ids)}
            {problemTable("No Raft Leader", problemRanges.no_raft_leader_range_ids)}
            {problemTable("No Lease", problemRanges.no_lease_range_ids)}
            {problemTable("Raft Leader but not Lease Holder", problemRanges.raft_leader_not_lease_holder_range_ids)}
            {problemTable("Underreplicated", problemRanges.underreplicated_range_ids)}
          </div>
        }
      </div>
    );
  }
}

export default connect(
  (state: AdminUIState) => {
    return {
      problemRanges: state.cachedData.problemRanges.data,
    };
  },
  {
    refreshProblemRanges,
  },
)(ProblemRanges);
