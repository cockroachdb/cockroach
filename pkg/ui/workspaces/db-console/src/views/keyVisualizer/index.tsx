// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  TimeScale,
  TimeScaleDropdown,
  TimeScaleOptions,
  util,
} from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import React from "react";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { refreshSettings } from "src/redux/apiReducers";
import { selectClusterSettings } from "src/redux/clusterSettings";
import { AdminUIState } from "src/redux/state";
import { selectTimeScale, setTimeScale } from "src/redux/timeScale";
import { getKeyVisualizerSamples } from "src/util/api";
import {
  KeyVisSample,
  KeyVisualizerProps,
} from "src/views/keyVisualizer/interfaces";
import KeyVisualizer from "src/views/keyVisualizer/keyVisualizer";

import { BackToAdvanceDebug } from "../reports/containers/util";

import { CanvasHeight, XAxisPadding } from "./constants";

import KeyVisSamplesRequest = cockroach.server.serverpb.KeyVisSamplesRequest;
import KeyVisSamplesResponse = cockroach.server.serverpb.KeyVisSamplesResponse;

const EnabledSetting = "keyvisualizer.enabled";
const IntervalSetting = "keyvisualizer.sample_interval";

const timeScaleOptions: TimeScaleOptions = {
  "Past 30 Minutes": {
    windowSize: moment.duration(30, "minutes"),
    windowValid: moment.duration(15, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
  "Past Hour": {
    windowSize: moment.duration(1, "hour"),
    windowValid: moment.duration(15, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
  "Past 6 Hours": {
    windowSize: moment.duration(6, "hours"),
    windowValid: moment.duration(15, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
  "Past Day": {
    windowSize: moment.duration(1, "day"),
    windowValid: moment.duration(15, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
  "Past 2 Days": {
    windowSize: moment.duration(2, "day"),
    windowValid: moment.duration(15, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
  "Past 3 Days": {
    windowSize: moment.duration(3, "day"),
    windowValid: moment.duration(15, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
  "Past Week": {
    windowSize: moment.duration(7, "days"),
    windowValid: moment.duration(15, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
};

export function getRequestsAsNumber(requests: number | Long | null): number {
  if (typeof requests === "number") {
    return requests;
  }

  return requests?.toInt() ?? 0;
}

function hottestBucket(samples: KeyVisSamplesResponse["samples"]) {
  let highest = 0;
  for (const sample of samples) {
    for (const stat of sample.buckets) {
      const numRequests = getRequestsAsNumber(stat.requests);
      if (numRequests > highest) {
        highest = numRequests;
      }
    }
  }
  return highest;
}

function encodeToHexString(bytes: Uint8Array): string {
  return Array.from(bytes, byte => {
    return ("0" + (byte & 0xff).toString(16)).slice(-2);
  }).join("");
}

// buildYAxis returns a mapping of pretty keys to the y coordinate of the key.
function buildYAxis(sortedPrettyKeys: string[]): Record<string, number> {
  // TODO(zachlite): make this faster
  // const sortedPrettyKeysInWindow = state.response.sorted_pretty_keys.filter((prettyKey) => {
  //   // is this key in the time window?
  //   for (const sample of samples) {
  //     for (const bucket of sample.buckets) {
  //
  //       const startPretty = state.response.pretty_key_for_uuid[bucket.start_key_id]
  //       const endPretty = state.response.pretty_key_for_uuid[bucket.end_key_id]
  //
  //       if (startPretty === prettyKey || endPretty === prettyKey) {
  //         return true
  //       }
  //     }
  //   }
  //   return false;
  // })

  // compute y offset for each key
  const yOffsetsForKey = {} as Record<string, number>;
  sortedPrettyKeys.forEach((key, i) => {
    yOffsetsForKey[key] =
      (i * (CanvasHeight - XAxisPadding)) / (sortedPrettyKeys.length - 1);
  });

  return yOffsetsForKey;
}

interface KeyVisualizerContainerState {
  response: KeyVisSamplesResponse;
}

function buildKeyVisualizerProps(
  state: KeyVisualizerContainerState,
  timeScale: TimeScale,
): KeyVisualizerProps {
  // The time window is in units of seconds.
  const windowEndSeconds =
    timeScale.fixedWindowEnd === false
      ? Date.now() / 1000
      : timeScale.fixedWindowEnd.unix();

  const windowStartSeconds =
    windowEndSeconds - timeScale.windowSize.asSeconds();

  // Filter out samples that fall outside the selected time window.
  const samples = state.response.samples
    .filter(sample => {
      const sampleTime = sample.timestamp.seconds.toNumber();
      return sampleTime >= windowStartSeconds && sampleTime <= windowEndSeconds;
    })
    .sort(
      (a, b) => a.timestamp.seconds.toNumber() - b.timestamp.seconds.toNumber(),
    );

  // Hex encode bucket UUIDs.
  const keySamples: KeyVisSample[] = samples.map(sample => ({
    ...sample,
    buckets: sample.buckets.map(bucket => ({
      ...bucket,
      startKeyHex: encodeToHexString(bucket.start_key_id),
      endKeyHex: encodeToHexString(bucket.end_key_id),
    })),
  }));

  return {
    keys: state.response.pretty_key_for_uuid,
    samples: keySamples,
    yOffsetsForKey: buildYAxis(state.response.sorted_pretty_keys),
    hottestBucket: hottestBucket(samples),
  };
}

interface KeyVisualizerContainerProps {
  refreshInterval: number;
  timeScale: TimeScale;
  setTimeScale: typeof setTimeScale;
}

class KeyVisualizerContainer extends React.Component<
  KeyVisualizerContainerProps & RouteComponentProps,
  KeyVisualizerContainerState
> {
  interval: any;

  state = { response: new KeyVisSamplesResponse() };

  fetchSamples() {
    const req = new KeyVisSamplesRequest({});
    getKeyVisualizerSamples(req).then(res => this.setState({ response: res }));
  }

  componentDidMount() {
    // set up a recurring sample refresh
    this.interval = setInterval(() => {
      this.fetchSamples();
    }, this.props.refreshInterval);

    // do an initial fetch
    this.fetchSamples();
  }

  componentWillUnmount() {
    clearInterval(this.interval);
  }

  render() {
    const { samples, yOffsetsForKey, hottestBucket, keys } =
      buildKeyVisualizerProps(this.state, this.props.timeScale);

    if (
      this.state.response.samples.length === 0 ||
      Object.keys(this.state.response.pretty_key_for_uuid).length === 0
    ) {
      return (
        <>
          <BackToAdvanceDebug history={this.props.history} />
          <div>Waiting for samples...</div>
        </>
      );
    }

    return (
      <>
        <BackToAdvanceDebug history={this.props.history} />
        <div style={{ position: "relative" }}>
          <TimeScaleDropdown
            options={timeScaleOptions}
            currentScale={this.props.timeScale}
            setTimeScale={this.props.setTimeScale}
          />
          <KeyVisualizer
            samples={samples}
            yOffsetsForKey={yOffsetsForKey}
            hottestBucket={hottestBucket}
            keys={keys}
          />
        </div>
      </>
    );
  }
}

interface KeyVisualizerPageProps {
  clusterSettings?: {
    [key: string]: cockroach.server.serverpb.SettingsResponse.IValue;
  };
  refreshSettings: typeof refreshSettings;
  setTimeScale: typeof setTimeScale;
  timeScale: TimeScale;
}

const KeyVisualizerPage: React.FunctionComponent<
  KeyVisualizerPageProps & RouteComponentProps
> = props => {
  if (props.clusterSettings === undefined) {
    props.refreshSettings();
    return null;
  }

  const enabled = props.clusterSettings[EnabledSetting].value === "true";

  if (!enabled) {
    return (
      <div>
        <BackToAdvanceDebug history={props.history} />
        <p>To enable the key visualizer, run the following SQL statement:</p>
        <pre>SET CLUSTER SETTING {EnabledSetting} = true;</pre>
      </div>
    );
  }

  const refreshInterval = util
    .durationFromISO8601String(props.clusterSettings[IntervalSetting].value)
    .asMilliseconds();

  return (
    <KeyVisualizerContainer
      {...props}
      timeScale={props.timeScale}
      refreshInterval={refreshInterval}
    />
  );
};

export default connect(
  (state: AdminUIState) => ({
    clusterSettings: selectClusterSettings(state),
    timeScale: selectTimeScale(state),
  }),
  {
    refreshSettings,
    setTimeScale,
  },
)(KeyVisualizerPage);
