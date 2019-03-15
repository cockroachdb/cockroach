// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import * as React from "react";

import * as protos from  "src/js/protos";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";
import "./sample.styl";

const durationOptions: DropdownOption[] = [
  { label: "1 sec", value: 1000000000 },
  { label: "10 secs", value: 10000000000 },
  { label: "20 secs", value: 20000000000 },
  { label: "30 secs", value: 30000000000 },
  { label: "60 secs", value: 60000000000 },
];

const targetCountOptions: DropdownOption[] = [
  { label: "10", value: 10 },
  { label: "50", value: 50 },
  { label: "100", value: 100 },
  { label: "250", value: 250 },
  { label: "500", value: 500 },
  { label: "1000", value: 1000 },
];

export class SampleState {
  duration = 1000000000;
  target_count = 50;
  active = false;
}

interface SampleProps {
  state: SampleState;
  onChange: (update: SampleQueryState) => void;
}

interface SampleButtonProps {
  sampleState: SampleState;
  startSampling: () => void;
}

function SampleButton(props: SampleButtonProps) {
  if (props.sampleState.active) {
    return (
        <button className="sample-button" disabled>Gathering Traces...</button>
    );
  }
  return (
      <button className="sample-button" onClick={props.startSampling}>Sample Traces</button>
  );
}

export class SampleOptions extends React.Component<SampleProps> {
  changeDuration = (selected: DropdownOption) => {
    this.props.state.duration = +selected.value;
    this.props.onChange(this.props.state);
  }

  changeTargetCount = (selected: DropdownOption) => {
    this.props.state.target_count = +selected.value;
    this.props.onChange(this.props.state);
  }

  startSampling = () => {
    this.props.state.active = true;
    this.props.onChange(this.props.state);
  }

  render() {
    return (
      <div className="sample-container">
        <Dropdown
          title="Sampling Duration"
          selected={this.props.state.duration.toString()}
          options={durationOptions}
          onChange={this.changeDuration}
        />
        <Dropdown
          title="Target Sample Count"
          selected={this.props.state.target_count.toString()}
          options={targetCountOptions}
          onChange={this.changeTargetCount}
        />
        <SampleButton
          sampleState={this.props.state}
          startSampling={this.startSampling}
        />
      </div>
    );
  }
}
