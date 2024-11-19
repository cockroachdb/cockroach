// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import debounce from "lodash/debounce";
import isEmpty from "lodash/isEmpty";
import React from "react";
import { Link } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { LivenessStatus } from "src/redux/nodes";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { generateLocalityRoute, getLocalityLabel } from "src/util/localities";
import { trustIcon } from "src/util/trust";
import InstructionsBox, {
  showInstructionsBox,
} from "src/views/clusterviz/components/instructionsBox";

import { CircleLayout } from "./circleLayout";
import { renderAsMap } from "./layout";
import { MapLayout } from "./mapLayout";

import arrowUpIcon from "!!raw-loader!assets/arrowUp.svg";

type Liveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

const BACK_BUTTON_OFFSET = 26;

interface NodeCanvasProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  livenessStatuses: { [id: string]: LivenessStatus };
  livenesses: { [id: string]: Liveness };
  tiers: LocalityTier[];
}

interface NodeCanvasState {
  viewportSize: [number, number];
}

export class NodeCanvas extends React.Component<
  NodeCanvasProps,
  NodeCanvasState
> {
  graphEl: React.RefObject<SVGSVGElement> = React.createRef();
  debouncedOnResize: () => void;

  constructor(props: any) {
    super(props);

    // Add debounced resize listener.
    this.debouncedOnResize = debounce(this.onResize, 200);
  }

  updateViewport = () => {
    const rect = this.graphEl.current.getBoundingClientRect();
    this.setState({
      viewportSize: [rect.width, rect.height],
    });
  };

  onResize = () => {
    this.updateViewport();
  };

  componentDidMount() {
    window.addEventListener("resize", this.debouncedOnResize);

    this.updateViewport();
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.debouncedOnResize);
  }

  renderContent(asMap: boolean) {
    if (!this.state) {
      return null;
    }

    const { localityTree, locationTree, livenessStatuses, livenesses } =
      this.props;
    const { viewportSize } = this.state;

    if (asMap) {
      return (
        <MapLayout
          localityTree={localityTree}
          locationTree={locationTree}
          livenessStatuses={livenessStatuses}
          viewportSize={viewportSize}
        />
      );
    }

    return (
      <CircleLayout
        viewportSize={viewportSize}
        localityTree={localityTree}
        livenessStatuses={livenessStatuses}
        livenesses={livenesses}
      />
    );
  }

  renderBackButton() {
    const { tiers } = this.props;

    if (!this.state || isEmpty(tiers)) {
      return null;
    }

    const parentLocality = tiers.slice(0, tiers.length - 1);

    return (
      <Link
        to={CLUSTERVIZ_ROOT + generateLocalityRoute(parentLocality)}
        style={{ textDecoration: "none", color: "#595f6c" }}
      >
        <div
          style={{
            position: "absolute",
            left: BACK_BUTTON_OFFSET,
            bottom: BACK_BUTTON_OFFSET,
            backgroundColor: "white",
            border: "1px solid #EDEDED",
            borderRadius: 3,
            padding: 12,
            boxShadow: "0px 0px 4px 0px rgba(0, 0, 0, 0.2)",
            letterSpacing: 0.5,
          }}
        >
          <span
            dangerouslySetInnerHTML={trustIcon(arrowUpIcon)}
            style={{ position: "relative", top: 1 }}
          />
          Up to{" "}
          <span style={{ textTransform: "uppercase" }}>
            {getLocalityLabel(parentLocality)}
          </span>
        </div>
      </Link>
    );
  }

  render() {
    const showMap = renderAsMap(
      this.props.locationTree,
      this.props.localityTree,
    );

    // We must render the SVG even before initializing the state, because we
    // need to read its dimensions from the DOM in order to initialize the
    // state.
    return (
      <div style={{ flexGrow: 1, position: "relative" }}>
        <div style={{ width: "100%", height: "100%", position: "absolute" }}>
          <svg
            style={{
              width: "100%",
              height: "100%",
              marginBottom: -3, // WHYYYYYYYYY?!?!?!?!?
              position: "absolute",
            }}
            className="cluster-viz"
            ref={this.graphEl}
          >
            {this.renderContent(showMap)}
          </svg>
        </div>
        {this.renderBackButton()}
        {showInstructionsBox(showMap, this.props.tiers) ? (
          <InstructionsBox />
        ) : null}
      </div>
    );
  }
}
