// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { connect } from "react-redux";
import cn from "classnames";

import { Breadcrumbs } from "src/views/clusterviz/containers/map/breadcrumbs";
import NeedEnterpriseLicense from "src/views/clusterviz/containers/map/needEnterpriseLicense";
import NodeCanvasContainer from "src/views/clusterviz/containers/map/nodeCanvasContainer";
import TimeScaleDropdown from "src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import swapByLicense from "src/views/shared/containers/licenseSwap";
import { parseLocalityRoute } from "src/util/localities";
import { Loading } from "@cockroachlabs/cluster-ui";
import { AdminUIState } from "src/redux/state";
import { selectEnterpriseEnabled } from "src/redux/license";
import { Dropdown } from "src/components/dropdown";
import { parseSplatParams } from "src/util/parseSplatParams";

const NodeCanvasContent = swapByLicense(
  NeedEnterpriseLicense,
  NodeCanvasContainer,
);

interface ClusterVisualizationProps {
  licenseDataExists: boolean;
  enterpriseEnabled: boolean;
  clusterDataError: Error | null;
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
        "cluster-visualization-layout__content-item--show-license": showingLicensePage,
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

export default withRouter(connect(mapStateToProps)(ClusterVisualization));
