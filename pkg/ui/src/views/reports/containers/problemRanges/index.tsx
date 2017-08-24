import _ from "lodash";
import Long from "long";
import React from "react";
import { connect } from "react-redux";
import { Link, RouterState } from "react-router";

import * as protos from "src/js/protos";
import { refreshProblemRanges } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import ConnectionsTable from "src/views/reports/containers/problemRanges/connectionsTable";

interface ProblemRangesOwnProps {
  problemRanges: protos.cockroach.server.serverpb.ProblemRangesResponse;
  refreshProblemRanges: typeof refreshProblemRanges;
}

type ProblemRangesProps = ProblemRangesOwnProps & RouterState;

function ProblemTableCell(props: { rangeIDs: string[] }) {
  return (
    <td className="problems-table__cell">
      {
        _.map(props.rangeIDs, (id) => {
          return (
            <Link key={id} className="problems-link" to={`reports/range/${id}`}>
              {id}
            </Link>
          );
        })
      }
    </td>
  );
}

function ProblemTable(props: {
  name: string,
  problems: protos.cockroach.server.serverpb.ProblemRangesResponse.NodeProblems$Properties[],
  extract: (p: protos.cockroach.server.serverpb.ProblemRangesResponse.NodeProblems$Properties) => Long[],
}) {
  const ids = _.flatMap(props.problems, (problem) => props.extract(problem));
  ids.sort((a, b) => a.compare(b));
  const uniqueIDs = _.chain(ids)
    .map(id => id.toString())
    .sortedUniq()
    .value();
  if (_.isEmpty(uniqueIDs)) {
    return null;
  }
  return (
    <div>
      <h2>{props.name}</h2>
      <table className="problems-table">
        <tbody>
          <tr className="problems-table__row">
            <ProblemTableCell rangeIDs={uniqueIDs} />
          </tr>
        </tbody>
      </table>
    </div>
  );
}

function extractRangeIDsByType(value: string) {
  return function (problems: protos.cockroach.server.serverpb.ProblemRangesResponse.NodeProblems$Properties) {
    if (!_.isEmpty(problems.error_message)) {
      return [];
    }
    return _.get(problems, value, []) as Long[];
  };
}

/**
 * Renders the Problem Ranges page.
 */
class ProblemRanges extends React.Component<ProblemRangesProps, {}> {
  refresh(props = this.props) {
    props.refreshProblemRanges(new protos.cockroach.server.serverpb.ProblemRangesRequest({
      node_id: props.params[nodeIDAttr],
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

    let titleText = "Problem Ranges for ";
    const validIDs = _.keys(_.pickBy(problemRanges.problems_by_node_id, problems => {
        return _.isEmpty(problems.error_message);
      }));

    switch (validIDs.length) {
      case 0:
        if (_.isEmpty(this.props.params[nodeIDAttr])) {
          titleText = `No nodes returned any results`;
        } else {
          titleText = `No results for node n${this.props.params[nodeIDAttr]}`;
        }
        break;
      case 1:
        const singleNodeID = _.keys(problemRanges.problems_by_node_id)[0];
        titleText = `Problem Ranges on Node n${singleNodeID}`;
        break;
      default:
        titleText = "Problem Ranges on the Cluster";
    }

    if (validIDs.length === 0) {
      return (
        <div className="section">
          <h1>{titleText}</h1>
          <ConnectionsTable problemRanges={problemRanges} />
        </div>
      );
    }

    const problems = _.values(problemRanges.problems_by_node_id);

    return (
      <div className="section">
        <h1>{titleText}</h1>
        <ProblemTable name="Unavailable"
          problems={problems}
          extract={extractRangeIDsByType("unavailable_range_ids")}
        />
        <ProblemTable name="No Raft Leader"
          problems={problems}
          extract={extractRangeIDsByType("no_raft_leader_range_ids")}
        />
        <ProblemTable name="No Lease"
          problems={problems}
          extract={extractRangeIDsByType("no_lease_range_ids")}
        />
        <ProblemTable name="Raft Leader but not Lease Holder"
          problems={problems}
          extract={extractRangeIDsByType("raft_leader_not_lease_holder_range_ids")}
        />
        <ProblemTable name="Underreplicated (or slow)"
          problems={problems}
          extract={extractRangeIDsByType("underreplicated_range_ids")}
        />
        <ConnectionsTable problemRanges={problemRanges} />
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
