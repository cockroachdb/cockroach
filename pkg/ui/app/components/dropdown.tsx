import Select from "react-select";
import * as React from "react";

import {leftArrow, rightArrow} from "./icons";
import { trustIcon } from "../util/trust";

export interface DropdownOption {
  value: string;
  label: string;
}

export enum ArrowDirection {
  LEFT, RIGHT
}

interface DropdownOwnProps {
  title: string;
  selected: string;
  options: DropdownOption[];
  onChange?: (selected: DropdownOption) => void; // Callback when the value changes.
  // If true, don't display the arrow next to the dropdown, display left and
  // right arrows to either side instead.
  sideArrows?: boolean;
  onArrowClick?: (direction: ArrowDirection) => void;
}

/**
 * Dropdown component that uses the URL query string for state.
 */
export default class Dropdown extends React.Component<DropdownOwnProps, {}> {
  render() {
    let {selected, options, onChange} = this.props;
    return <div className="dropdown">
      {/* TODO (maxlang): consider moving arrows outside the dropdown component */}
      <span className="dropdown__side-arrow" dangerouslySetInnerHTML={trustIcon(leftArrow)} onClick={() => this.props.onArrowClick(ArrowDirection.LEFT)}></span>
      <span className="dropdown__title">{this.props.title}{this.props.title ? ":" : ""}</span>
      <Select className="dropdown__select" clearable={false} options={options} value={selected} onChange={onChange} />
      <span className="dropdown__side-arrow" dangerouslySetInnerHTML={trustIcon(rightArrow)} onClick={() => this.props.onArrowClick(ArrowDirection.RIGHT)}></span>
    </div>;
  }
}
