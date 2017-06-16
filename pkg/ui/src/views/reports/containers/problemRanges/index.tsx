import React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { refreshReportProblemRanges } from "src/redux/apiReducers";
import { RouterState } from "react-router";

interface ProblemRangesOwnProps {
  problemRanges: protos.cockroach.server.serverpb.ReportProblemRangesResponse;
  refreshReportProblemRanges: typeof refreshReportProblemRanges;
}

type ProblemRangesProps = ProblemRangesOwnProps & RouterState;

/**
 * Renders the Problem Ranges page.
 */
class ProblemRanges extends React.Component<ProblemRangesProps, {}> {
  refresh(props = this.props) {
    props.refreshReportProblemRanges(new protos.cockroach.server.serverpb.ReportProblemRangesRequest({
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
          <table className="failure-table">
            <thead>
              <tr className="failure-table__row failure-table__row--header">
                <th className="failure-table__cell failure-table__cell--short">Node</th>
                <th className="failure-table__cell">Error</th>
              </tr>
            </thead>
            <tbody>
              {
                _.map(problemRanges.failures, (failure) => (
                  <tr className="failure-table__row" key={failure.node_id}>
                    <td className="failure-table__cell failure-table__cell--short">n{failure.node_id}</td>
                    <td className="failure-table__cell">title={failure.error_message}>{failure.error_message}</td>
                  </tr>
                ))
              }
            </tbody>
          </table>
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
          <table className="problems-table">
            <tbody>
              <tr className="problems-table__row">
                <td className="problems-table__cell">
                  {
                    _.map(ids, (id) => (
                      <span key={id.toNumber()}>
                        <a href={`/debug/range?id=${id}`}>{id.toNumber()}</a>
                        <span> </span>
                      </span>
                    ))
                  }
                </td>
              </tr>
            </tbody>
          </table>
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
      problemRanges: state.cachedData.reportProblemRanges.data,
    };
  },
  {
    refreshReportProblemRanges,
  },
)(ProblemRanges);
