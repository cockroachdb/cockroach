import _ from "lodash";
import Select from "react-select";
import * as React from "react";
import { IInjectedProps, IRouter } from "react-router";

export interface SelectorOption {
  value: string;
  label: string;
  default?: boolean;
}

interface SelectorOwnProps {
  title: string;
  urlKey: string; // The URL key used to retrieve/save the state.
  options: SelectorOption[];
  onChange?: (selected: SelectorOption) => void; // Callback when the value changes.
}

class SelectorState {
  selected: number;
}

/**
 * Selector component that uses the URL query string for state.
 */
export default class Selector extends React.Component<SelectorOwnProps, SelectorState> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  static contextTypes = {
    router: React.PropTypes.object.isRequired,
  };
  context: { router?: IRouter & IInjectedProps; };

  state = new SelectorState();

  componentWillMount() {
    this.propsChange();
  }

  componentWillReceiveProps(props: SelectorOwnProps) {
    this.propsChange(props);
  }

  propsChange(props = this.props) {
    let selected: number;
    let query: any = this.context.router.location.query;
    // If there are no options, wait for options to be populated.
    if (!props.options || !props.options.length) {
      return;
    // If a value is specified in the URL, set that value
    } else if (_.has(query, props.urlKey)) {
      selected = _.findIndex(props.options, { value: query[props.urlKey] });
    // Otherwise use the default value or, barring that, the first value.
    } else {
      selected = _.findIndex(props.options, { default: true });
      if (selected === -1) {
        selected = 0;
      }
    }
    if (this.state.selected !== selected ) {
      this.setState({ selected });
      this.props.onChange(props.options[selected]);
    }
  }

  onChange = (selected: SelectorOption) => {
    let location = _.clone(this.context.router.location);
    (location.query as any)[this.props.urlKey] = selected.value;
    this.context.router.push(location);
    this.props.onChange(selected);
  }

  render() {
    let selected = this.props.options && _.isNumber(this.state.selected) && this.props.options[this.state.selected] || null;
    let options = this.props.options || [];
    return <div className="dropdown-option">
      <span className="dropdown-option__title">{ this.props.title }:</span>
      <Select className="dropdown-option__select" clearable={false} options={options} value={selected} onChange={this.onChange} />
    </div>;
  }
}
