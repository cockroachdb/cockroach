import classNames from "classnames";
import React from "react";
import { connect } from "react-redux";

import { AdminUIState } from "src/redux/state";
import { AggregationLevel, selectAggregationLevel, setAggregationLevel } from "src/redux/aggregationLevel";

import "./toggle.styl";

interface AggregationSelectorProps {
  aggregationLevel: AggregationLevel,
  setAggregationLevel: typeof setAggregationLevel,
}

/**
 * A component to choose the level at which aggregation takes place,
 */
class AggregationSelector extends React.Component<AggregationSelectorProps> {
  setClusterAggregation = () => {
    if (this.props.aggregationLevel !== AggregationLevel.Cluster) {
      this.props.setAggregationLevel(AggregationLevel.Cluster);
    }
  }

  setPerNodeAggregation = () => {
    if (this.props.aggregationLevel !== AggregationLevel.Node) {
      this.props.setAggregationLevel(AggregationLevel.Node);
    }
  }

  render() {
    const { aggregationLevel } = this.props;

    const clusterClasses = classNames("toggle__option", { "toggle__option--selected": aggregationLevel === AggregationLevel.Cluster });
    const perNodeClasses = classNames("toggle__option", { "toggle__option--selected": aggregationLevel === AggregationLevel.Node });

    return <div className="toggle">
      <span className={clusterClasses} onClick={this.setClusterAggregation}>Cluster</span>
      <span className={perNodeClasses} onClick={this.setPerNodeAggregation}>Per Node</span>
    </div>;
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    aggregationLevel: selectAggregationLevel(state),
  };
}

const actions = {
  setAggregationLevel,
};

export default connect(mapStateToProps, actions)(AggregationSelector);
