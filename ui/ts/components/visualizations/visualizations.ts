/// <reference path="./bar.ts" />
/// <reference path="./bullet.ts" />
/// <reference path="./line.ts" />
/// <reference path="./number.ts" />
/// <reference path="./pie.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Visualizations {
  "use strict";
  import ParameterizedMithrilComponent = _mithril.ParameterizedMithrilComponent;

  export interface VisualizationData {
    key?: string;
    value: number;
  }

  export interface VisualizationInfo {
    width: number;
    height: number;
    title?: string;
    data: VisualizationData | VisualizationData[];
    visualizationComponent?: ParameterizedMithrilComponent<any, VisualizationInfo>;
    visualizationArguments?: any;
  }

  export module VisualizationWrapper {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    export function controller(): void {
    }

    export function view(ctrl: any, info: VisualizationInfo): MithrilVirtualElement {

      // Layout Constants. Change these values in visualizations.styl if you change them here.
      let padding = {
        top: 30,
        bottom: 65,
        left: 62,
        right: 62
      };

      let wrappedVisualizationInfo = _.cloneDeep(info);
      wrappedVisualizationInfo.width -= padding.left + padding.right;
      wrappedVisualizationInfo.height -= padding.top + padding.bottom;

      return m(
        ".visualization-wrapper",
        {style: "width:" + info.width.toString() + "px; height:" + info.height.toString() + "px;"},
        [
          // TODO: pass in and display info icon tooltip
          m(".viz-top", m(".viz-info-icon", m(".icon-cockroach-17"))), // Icon Cockroach 17 is the info icon.
          m.component(info.visualizationComponent, wrappedVisualizationInfo),
          m(".viz-bottom", m(".viz-title", info.title))
        ]
      );
    }
  }

}
