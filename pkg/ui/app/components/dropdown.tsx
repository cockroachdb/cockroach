import Select from "react-select";
import * as React from "react";

export interface SelectorOption {
  value: string;
  label: string;
}

interface SelectorOwnProps {
  title: string;
  selected: string;
  options: SelectorOption[];
  onChange?: (selected: SelectorOption) => void; // Callback when the value changes.
}

/**
 * Selector component that uses the URL query string for state.
 */
export default class Selector extends React.Component<SelectorOwnProps, {}> {
  render() {
    let {selected, options, onChange} = this.props;
    return <div className="dropdown">
      <span className="dropdown__title">{ this.props.title }:</span>
      <Select className="dropdown__select" clearable={false} options={options} value={selected} onChange={onChange} />
    </div>;
  }
}
