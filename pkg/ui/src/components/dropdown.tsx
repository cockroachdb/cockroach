import Select from "react-select";
import * as React from "react";
import _ from "lodash";

import {leftArrow, rightArrow} from "./icons";
import { trustIcon } from "../util/trust";

export interface DropdownOption {
  value: string;
  label: string;
}

export enum ArrowDirection {
  LEFT, RIGHT,
}

interface DropdownOwnProps {
  title: string;
  selected: string;
  options: DropdownOption[];
  onChange?: (selected: DropdownOption) => void; // Callback when the value changes.
  // If onArrowClick exists, don't display the arrow next to the dropdown,
  // display left and right arrows to either side instead.
  onArrowClick?: (direction: ArrowDirection) => void;
  // Disable any arrows in the arrow direction array.
  disabledArrows?: ArrowDirection[];
}

/**
 * Dropdown component that uses the URL query string for state.
 */
export default class Dropdown extends React.Component<DropdownOwnProps, {}> {
  render() {
    let {selected, options, onChange, onArrowClick, disabledArrows} = this.props;
    let className = "dropdown";
    if (onArrowClick) {
      className += " dropdown--side-arrows";
    }
    return <div className={className}>
      {/* TODO (maxlang): consider moving arrows outside the dropdown component */}
      <span
        className="dropdown__side-arrow"
        disabled={_.includes(disabledArrows, ArrowDirection.LEFT)}
        dangerouslySetInnerHTML={trustIcon(leftArrow)}
        onClick={() => this.props.onArrowClick(ArrowDirection.LEFT)}>
      </span>
      <span className="dropdown__title">{this.props.title}{this.props.title ? ":" : ""}</span>
      <Select className="dropdown__select" clearable={false} searchable={false} options={options} value={selected} onChange={onChange} />
      <span
        className="dropdown__side-arrow"
        disabled={_.includes(disabledArrows, ArrowDirection.RIGHT)}
        dangerouslySetInnerHTML={trustIcon(rightArrow)}
        onClick={() => this.props.onArrowClick(ArrowDirection.RIGHT)}>
      </span>
    </div>;
  }
}
