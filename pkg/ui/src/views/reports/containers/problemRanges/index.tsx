import _ from "lodash";
import Long from "long";
import React from "react";
import { connect } from "react-redux";
import { Link, RouterState } from "react-router";

import * as protos from "src/js/protos";
import { refreshProblemRanges } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import ConnectionsTable from "src/views/reports/containers/problemRanges/connectionsTable";

type ProblemRangesResponse = protos.cockroach.server.serverpb.ProblemRangesResponse;
type NodeProblems$Properties = protos.cockroach.server.serverpb.ProblemRangesResponse.NodeProblems$Properties;

interface ProblemRangesOwnProps {
  problemRanges: ProblemRangesResponse;
  refreshProblemRanges: typeof refreshProblemRanges;
}

type ProblemRangesProps = ProblemRangesOwnProps & RouterState;

function ProblemRangeList(props: {
  name: string,
  problems: NodeProblems$Properties[],
  extract: (p: NodeProblems$Properties) => Long[],
}) {
  const ids = _.chain(props.problems)
    .filter(problem => _.isEmpty(problem.error_message))
    .flatMap(problem => props.extract(problem))
    .map(id => FixLong(id))
    .sort((a, b) => a.compare(b))
    .map(id => id.toString())
    .sortedUniq()
    .value();
  if (_.isEmpty(ids)) {
    return null;
  }
  return (
    <div>
      <h2>{props.name}</h2>
      <div className="problems-list">
        {
          _.map(ids, id => {
            return (
              <Link key={id} className="problems-link" to={`reports/range/${id}`}>
                {id}
              </Link>
            );
          })
        }
      </div>
    </div>
  );
}

/**
 * Renders the Problem Ranges page.
 *
 * The problem ranges endpoint returns a list of known ranges with issues on a
 * per node basis. This page aggregates those lists together and displays all
 * unique range IDs that have problems.
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
          <h1>Problem Ranges Report</h1>
          <h2>Loading cluster status...</h2>
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
          <h1>Problem Ranges Report</h1>
          <h2>{titleText}</h2>
          <ConnectionsTable problemRanges={problemRanges} />
        </div>
      );
    }

    const problems = _.values(problemRanges.problems_by_node_id);
    return (
      <div className="section">
        <h1>Problem Ranges Report</h1>
        <h2>{titleText}</h2>
        <ProblemRangeList
          name="Unavailable"
          problems={problems}
          extract={(problem) => problem.unavailable_range_ids}
        />
        <ProblemRangeList
          name="No Raft Leader"
          problems={problems}
          extract={(problem) => problem.no_raft_leader_range_ids}
        />
        <ProblemRangeList
          name="No Lease"
          problems={problems}
          extract={(problem) => problem.no_lease_range_ids}
        />
        <ProblemRangeList
          name="Raft Leader but not Lease Holder"
          problems={problems}
          extract={(problem) => problem.raft_leader_not_lease_holder_range_ids}
        />
        <ProblemRangeList
          name="Underreplicated (or slow)"
          problems={problems}
          extract={(problem) => problem.underreplicated_range_ids}
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
