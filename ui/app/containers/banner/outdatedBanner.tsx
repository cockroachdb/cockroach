import * as React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import moment = require("moment");
import _ = require("lodash");

import Banner from "./banner";
import { AdminUIState } from "../../redux/state";
import { refreshNodes, refreshCluster, refreshVersion } from "../../redux/apiReducers";
import { VERSION_DISMISSED_KEY, loadUIData, saveUIData } from "../../redux/uiData";
import { VersionList } from "../../interfaces/cockroachlabs";
import { NodeStatus } from "../../util/proto";

const MILLISECONDS_IN_DAY = moment.duration(1, "day").asMilliseconds();

class OutdatedBannerProps {
  clusterID: string;
  buildtag: string;
  versionsMatch: boolean;
  nodeCount: number;
  versionCount: number;
  newerVersions: VersionList;
  versionCheckDismissedFetched: boolean;
  versionCheckDismissed: number;

  refreshVersion: typeof refreshVersion;
  refreshCluster: typeof refreshCluster;
  refreshNodes: typeof refreshNodes;
  loadUIData: typeof loadUIData;
  saveUIData: typeof saveUIData;
}

class OutdatedBannerState {
  dismissed: boolean = false;
}

class OutdatedBanner extends React.Component<OutdatedBannerProps, OutdatedBannerState> {
  state = new OutdatedBannerState();

  getNecessaryInfo(props: OutdatedBannerProps = this.props) {
    props.refreshNodes();

    if (!props.versionCheckDismissedFetched) {
      props.loadUIData(VERSION_DISMISSED_KEY);
    }

    if (!props.clusterID) {
      props.refreshCluster();
    }

    if (props.clusterID && props.buildtag && !props.newerVersions) {
      props.refreshVersion({ clusterID: props.clusterID, buildtag: props.buildtag });
    }
  }

  componentWillMount() {
    this.getNecessaryInfo();
  }

  componentWillReceiveProps(props: OutdatedBannerProps) {
    this.getNecessaryInfo(props);
  }

  dismiss() {
    this.props.saveUIData({ key: VERSION_DISMISSED_KEY, value: Date.now() });
    this.setState({ dismissed: true });
  }

  bannerText() {
    if (!this.props.versionsMatch) {
      return `Node versions are mismatched. ${this.props.versionCount} versions were detected across ${this.props.nodeCount} nodes.`;
    } else if (this.props.newerVersions && this.props.newerVersions.details && this.props.newerVersions.details.length) {
      return `There is a newer version of CockroachDB available.`;
    }
  }

  visible() {
    // Check if the banner was dismissed on the client (might still be saving to the server.)
    return !this.state.dismissed &&
      // Check if the banner was dismissed in the last day.
      (this.props.versionCheckDismissedFetched && !this.props.versionCheckDismissed || (Date.now() - this.props.versionCheckDismissed > MILLISECONDS_IN_DAY)) &&
      // Check if it would show any text (i.e. only if there's a version mismatch or a newer version.)
      this.bannerText() !== undefined;
  }

  render() {
    let visible: boolean = this.visible();
    return <Banner className="outdated" visible={visible} onclose={() => this.dismiss() }>
      <span className="icon-warning" />
      { this.bannerText() }
      <a href="https://www.cockroachlabs.com/docs/install-cockroachdb.html" target="_blank">
        <button>Update</button>
      </a>
    </Banner>;
  }
}

let nodeStatuses = (state: AdminUIState): NodeStatus[] => state.cachedData.nodes.data;
let newerVersions = (state: AdminUIState): VersionList => state.cachedData.version.valid ? state.cachedData.version.data : null;
let clusterID = (state: AdminUIState): string => state.cachedData.cluster.data && state.cachedData.cluster.data.cluster_id;

// TODO(maxlang): change the way uiData stores data so we don't need an added check to see if the value was fetched
// TODO(maxlang): determine whether they dismissed a node version mismatch or an upstream version check
let versionCheckDismissedFetched = (state: AdminUIState): boolean => state.uiData.data && _.has(state.uiData.data, VERSION_DISMISSED_KEY);
let versionCheckDismissed = (state: AdminUIState): number => versionCheckDismissedFetched(state) ? state.uiData.data[VERSION_DISMISSED_KEY] : undefined;

let versions = createSelector(
  nodeStatuses,
  (statuses: NodeStatus[]): string[] => statuses && _.uniq(_.map(statuses, (s: NodeStatus) => s.build_info.tag))
);

let nodeCount = createSelector(
  nodeStatuses,
  (statuses: NodeStatus[]) => statuses && statuses.length
);

let versionCount = createSelector(
  versions,
  (v: string[]) => v && v.length
);

let versionsMatch = createSelector(
  versions,
  (v: string[]) => v && v.length === 1
);

let buildtag = createSelector(
  versions,
  (v: string[]): string => v && v.length === 1 ? v[0] : null
);

// Connect the DisconnectedBanner class with our redux store.
let outdatedBannerConnected = connect(
  (state: AdminUIState) => {
    return {
      clusterID: clusterID(state),
      buildtag: buildtag(state),
      versionsMatch: versionsMatch(state),
      nodeCount: nodeCount(state),
      versionCount: versionCount(state),
      newerVersions: newerVersions(state),
      versionCheckDismissedFetched: versionCheckDismissedFetched(state),
      versionCheckDismissed: versionCheckDismissed(state),
    };
  },
  {
    refreshCluster,
    refreshVersion,
    refreshNodes,
    loadUIData,
    saveUIData,
  }
)(OutdatedBanner);

export default outdatedBannerConnected;
