// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading } from "@cockroachlabs/cluster-ui";
import cn from "classnames";
import React from "react";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { Dropdown } from "src/components/dropdown";
import { refreshCluster } from "src/redux/apiReducers";
import { selectEnterpriseEnabled } from "src/redux/license";
import { AdminUIState } from "src/redux/state";
import { parseLocalityRoute } from "src/util/localities";
import { parseSplatParams } from "src/util/parseSplatParams";
import TimeScaleDropdown from "src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import { Breadcrumbs } from "src/views/clusterviz/containers/map/breadcrumbs";
import NeedEnterpriseLicense from "src/views/clusterviz/containers/map/needEnterpriseLicense";
import NodeCanvasContainer from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import swapByLicense from "src/views/shared/containers/licenseSwap";

const NodeCanvasContent = swapByLicense(
  NeedEnterpriseLicense,
  NodeCanvasContainer,
);

interface ClusterVisualizationProps {
  licenseDataExists: boolean;
  enterpriseEnabled: boolean;
  clusterDataError: Error | null;
  refreshCluster: typeof refreshCluster;
}

export class ClusterVisualization extends React.Component<
  ClusterVisualizationProps & RouteComponentProps
> {
  readonly items = [
    { value: "list", name: "Node List" },
    { value: "map", name: "Node Map" },
  ];

  handleMapTableToggle = (value: string) => {
    this.props.history.push(`/overview/${value}`);
  };

  getTiers() {
    const { match, location } = this.props;
    const splat = parseSplatParams(match, location);
    return parseLocalityRoute(splat);
  }

  componentDidMount() {
    this.props.refreshCluster();
  }

  componentDidUpdate() {
    this.props.refreshCluster();
  }

  render() {
    const tiers = this.getTiers();

    // TODO(couchand): integrate with license swapper
    const showingLicensePage =
      this.props.licenseDataExists && !this.props.enterpriseEnabled;

    const classes = cn("cluster-visualization-layout", {
      "cluster-visualization-layout--show-license": showingLicensePage,
    });

    const contentItemClasses = cn(
      "cluster-visualization-layout__content-item",
      {
        "cluster-visualization-layout__content-item--show-license":
          showingLicensePage,
      },
    );

    // TODO(vilterp): dedup with NodeList
    return (
      <div className={classes}>
        <div className="cluster-visualization-layout__content">
          <div className="cluster-visualization-layout__content-item">
            <Dropdown items={this.items} onChange={this.handleMapTableToggle}>
              Node Map
            </Dropdown>
          </div>
          <div className={contentItemClasses}>
            <Breadcrumbs tiers={tiers} />
          </div>
          <div className={contentItemClasses}>
            <TimeScaleDropdown />
          </div>
        </div>
        <Loading
          loading={!this.props.licenseDataExists}
          page={"containers"}
          error={this.props.clusterDataError}
          render={() => <NodeCanvasContent tiers={tiers} />}
        />
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    licenseDataExists: !!state.cachedData.cluster.data,
    enterpriseEnabled: selectEnterpriseEnabled(state),
    clusterDataError: state.cachedData.cluster.lastError,
  };
}

export default withRouter(
  connect(mapStateToProps, {
    refreshCluster,
  })(ClusterVisualization),
);
