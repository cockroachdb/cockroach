// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import Long from "long";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { Link, RouteComponentProps, withRouter } from "react-router-dom";
import * as protos from "src/js/protos";
import {
  problemRangesRequestKey,
  refreshProblemRanges,
} from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";
import { FixLong } from "src/util/fixLong";
import ConnectionsTable from "src/views/reports/containers/problemRanges/connectionsTable";
import { Loading } from "@cockroachlabs/cluster-ui";
import { getMatchParamByName } from "src/util/query";

type NodeProblems$Properties = protos.cockroach.server.serverpb.ProblemRangesResponse.INodeProblems;

interface ProblemRangesOwnProps {
  problemRanges: CachedDataReducerState<
    protos.cockroach.server.serverpb.ProblemRangesResponse
  >;
  refreshProblemRanges: typeof refreshProblemRanges;
}

type ProblemRangesProps = ProblemRangesOwnProps & RouteComponentProps;

function isLoading(state: CachedDataReducerState<any>) {
  return _.isNil(state) || (_.isNil(state.data) && _.isNil(state.lastError));
}

function ProblemRangeList(props: {
  name: string;
  problems: NodeProblems$Properties[];
  extract: (p: NodeProblems$Properties) => Long[];
  description?: string;
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
      <h2 className="base-heading">{props.name}</h2>
      {props.description && (
        <div className="problems-description">{props.description}</div>
      )}
      <div className="problems-list">
        {_.map(ids, id => {
          return (
            <Link
              key={id}
              className="problems-link"
              to={`/reports/range/${id}`}
            >
              {id}
            </Link>
          );
        })}
      </div>
    </div>
  );
}

function problemRangeRequestFromProps(props: ProblemRangesProps) {
  return new protos.cockroach.server.serverpb.ProblemRangesRequest({
    node_id: getMatchParamByName(props.match, nodeIDAttr),
  });
}

/**
 * Renders the Problem Ranges page.
 *
 * The problem ranges endpoint returns a list of known ranges with issues on a
 * per node basis. This page aggregates those lists together and displays all
 * unique range IDs that have problems.
 */
export class ProblemRanges extends React.Component<ProblemRangesProps, {}> {
  refresh(props = this.props) {
    props.refreshProblemRanges(problemRangeRequestFromProps(props));
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refresh();
  }

  componentDidUpdate(prevProps: ProblemRangesProps) {
    if (!_.isEqual(this.props.location, prevProps.location)) {
      this.refresh(this.props);
    }
  }

  renderReportBody() {
    const { problemRanges, match } = this.props;
    const nodeId = getMatchParamByName(match, nodeIDAttr);

    if (isLoading(this.props.problemRanges)) {
      return null;
    }

    if (!_.isNil(problemRanges.lastError)) {
      if (nodeId === null) {
        return (
          <div>
            <h2 className="base-heading">
              Error loading Problem Ranges for the Cluster
            </h2>
            {problemRanges.lastError.toString()}
          </div>
        );
      } else {
        return (
          <div>
            <h2 className="base-heading">
              Error loading Problem Ranges for node n{nodeId}
            </h2>
            {problemRanges.lastError.toString()}
          </div>
        );
      }
    }

    const { data } = problemRanges;

    const validIDs = _.keys(
      _.pickBy(data.problems_by_node_id, d => {
        return _.isEmpty(d.error_message);
      }),
    );
    if (validIDs.length === 0) {
      if (nodeId === null) {
        return <h2 className="base-heading">No nodes returned any results</h2>;
      } else {
        return (
          <h2 className="base-heading">
            No results reported for node n{nodeId}
          </h2>
        );
      }
    }

    let titleText: string; // = "Problem Ranges for ";
    if (validIDs.length === 1) {
      const singleNodeID = _.keys(data.problems_by_node_id)[0];
      titleText = `Problem Ranges on Node n${singleNodeID}`;
    } else {
      titleText = "Problem Ranges on the Cluster";
    }

    const problems = _.values(data.problems_by_node_id);
    return (
      <div>
        <h2 className="base-heading">{titleText}</h2>
        <ProblemRangeList
          name="Unavailable"
          problems={problems}
          extract={problem => problem.unavailable_range_ids}
        />
        <ProblemRangeList
          name="No Raft Leader"
          problems={problems}
          extract={problem => problem.no_raft_leader_range_ids}
        />
        <ProblemRangeList
          name="Expired Lease"
          problems={problems}
          extract={problem => problem.no_lease_range_ids}
          description="Note that having expired leases is unlikely to be a problem. They can occur after node restarts and will clear on its own in up to 24 hours."
        />
        <ProblemRangeList
          name="Raft Leader but not Lease Holder"
          problems={problems}
          extract={problem => problem.raft_leader_not_lease_holder_range_ids}
        />
        <ProblemRangeList
          name="Underreplicated (or slow)"
          problems={problems}
          extract={problem => problem.underreplicated_range_ids}
        />
        <ProblemRangeList
          name="Overreplicated"
          problems={problems}
          extract={problem => problem.overreplicated_range_ids}
        />
        <ProblemRangeList
          name="Quiescent equals ticking"
          problems={problems}
          extract={problem => problem.quiescent_equals_ticking_range_ids}
        />
        <ProblemRangeList
          name="Circuit breaker error"
          problems={problems}
          extract={problem => problem.circuit_breaker_error_range_ids}
        />
      </div>
    );
  }

  render() {
    return (
      <div className="section">
        <Helmet title="Problem Ranges | Debug" />
        <h1 className="base-heading">Problem Ranges Report</h1>
        <Loading
          loading={isLoading(this.props.problemRanges)}
          page={"problems range"}
          error={this.props.problemRanges && this.props.problemRanges.lastError}
          render={() => (
            <div>
              {this.renderReportBody()}
              <ConnectionsTable problemRanges={this.props.problemRanges} />
            </div>
          )}
        />
      </div>
    );
  }
}

const mapStateToProps = (state: AdminUIState, props: ProblemRangesProps) => {
  const nodeIDKey = problemRangesRequestKey(
    problemRangeRequestFromProps(props),
  );
  return {
    problemRanges: state.cachedData.problemRanges[nodeIDKey],
  };
};

const mapDispatchToProps = {
  // actionCreators returns objects with type and payload
  refreshProblemRanges,
};

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(ProblemRanges),
);
