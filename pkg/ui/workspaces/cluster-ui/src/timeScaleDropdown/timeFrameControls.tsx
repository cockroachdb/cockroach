// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import { Button, Tooltip } from "antd";
import "antd/lib/button/style";
import "antd/lib/tooltip/style";
import { CaretLeft, CaretRight } from "@cockroachlabs/icons";
import { ArrowDirection } from "./timeScaleTypes";

import styles from "./timeFrameControls.module.scss";

const cx = classNames.bind(styles);

const ButtonGroup = Button.Group;

export interface RangeSelectProps {
  // If onArrowClick exists, don't display the arrow next to the dropdown,
  // display left and right arrows to either side instead.
  onArrowClick?: (direction: ArrowDirection) => void;
  // Disable any arrows in the arrow direction array.
  disabledArrows?: ArrowDirection[];
}

export const TimeFrameControls = ({
  onArrowClick,
  disabledArrows,
}: RangeSelectProps): React.ReactElement => {
  const handleChangeArrow = (direction: ArrowDirection) => () =>
    onArrowClick(direction);

  const left = disabledArrows.includes(ArrowDirection.LEFT);
  const right = disabledArrows.includes(ArrowDirection.RIGHT);
  const delay = 0.3;

  return (
    <div className={cx("controls-content")}>
      <ButtonGroup>
        <Tooltip
          placement="bottom"
          title="Previous time interval"
          mouseEnterDelay={delay}
          mouseLeaveDelay={delay}
        >
          <Button
            onClick={handleChangeArrow(ArrowDirection.LEFT)}
            disabled={left}
            className={cx("_action", left ? "disabled" : "active")}
            aria-label={"previous time interval"}
          >
            <CaretLeft className={cx("icon")} />
          </Button>
        </Tooltip>
        <Tooltip
          placement="bottom"
          title="Next time interval"
          mouseEnterDelay={delay}
          mouseLeaveDelay={delay}
        >
          <Button
            onClick={handleChangeArrow(ArrowDirection.RIGHT)}
            disabled={right}
            className={cx("_action", right ? "disabled" : "active")}
            aria-label={"next time interval"}
          >
            <CaretRight className={cx("icon")} />
          </Button>
        </Tooltip>
      </ButtonGroup>
      <Tooltip
        placement="bottom"
        title="Most recent interval"
        mouseEnterDelay={delay}
        mouseLeaveDelay={delay}
      >
        <Button
          onClick={handleChangeArrow(ArrowDirection.CENTER)}
          className={cx("_action", "active", "btn__now")}
        >
          Now
        </Button>
      </Tooltip>
    </div>
  );
};

export default TimeFrameControls;
