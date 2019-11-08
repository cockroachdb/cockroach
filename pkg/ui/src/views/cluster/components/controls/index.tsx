// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Button, Tooltip } from "antd";
import _ from "lodash";
import { ArrowDirection } from "oss/src/views/shared/components/dropdown";
import React from "react";
import "./controls.styl";

// tslint:disable-next-line: variable-name
const ButtonGroup = Button.Group;

interface RangeSelectProps {
  // If onArrowClick exists, don't display the arrow next to the dropdown,
  // display left and right arrows to either side instead.
  onArrowClick?: (direction: ArrowDirection) => void;
  // Disable any arrows in the arrow direction array.
  disabledArrows?: ArrowDirection[];
}

class TimeFrameControls extends React.Component<RangeSelectProps> {
  handleChangeArrow = (direction: ArrowDirection) => () => this.props.onArrowClick(direction);

  render() {
    const { disabledArrows } = this.props;
    const left = _.includes(disabledArrows, ArrowDirection.LEFT);
    const center = _.includes(disabledArrows, ArrowDirection.CENTER);
    const right = _.includes(disabledArrows, ArrowDirection.RIGHT);
    const delay = .3;
    return (
      <div className="controls-content">
        <ButtonGroup>
          <Tooltip placement="bottom" title="previous timeframe" mouseEnterDelay={delay} mouseLeaveDelay={delay}>
            <Button
              onClick={this.handleChangeArrow(ArrowDirection.LEFT)}
              disabled={left}
              className={`_action ${left ? "disabled" : "active"}`}
              icon="fast-backward"
            />
          </Tooltip>
          <Tooltip placement="bottom" title="Now" mouseEnterDelay={delay} mouseLeaveDelay={delay}>
            <Button
              onClick={this.handleChangeArrow(ArrowDirection.CENTER)}
              disabled={center}
              className={`_action ${center ? "disabled" : "active"}`}
              icon="caret-right"
            />
          </Tooltip>
          <Tooltip placement="bottom" title="next timeframe" mouseEnterDelay={delay} mouseLeaveDelay={delay}>
            <Button
              onClick={this.handleChangeArrow(ArrowDirection.RIGHT)}
              disabled={right}
              className={`_action ${right ? "disabled" : "active"}`}
              icon="fast-forward"
            />
          </Tooltip>
        </ButtonGroup>
      </div>
    );
  }
}

export default TimeFrameControls;
