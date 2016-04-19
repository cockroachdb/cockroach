// source: component/tooltip.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../util/types.ts" />

module Components {
  "use strict";

  /**
   * Tooltip is a component that creates a configurable tooltip
   */
  export module Tooltip {
    export interface TooltipConfig {
      // Class applied to the tooltip
      tooltipClass?: string;
      title?: MithrilChild;
      content: MithrilChild;
      icon?: string;
      position?: string;
    }

    export function controller(): any {}

    export function view(ctrl: any, tooltipConfig: TooltipConfig): _mithril.MithrilVirtualElement {
      return m(".tooltip" + (tooltipConfig.position || ".right") + (tooltipConfig.tooltipClass || ""), [
        tooltipConfig.icon ? m(tooltipConfig.icon) : m(""),
        m(".title", tooltipConfig.title ? tooltipConfig.title : ""),
        m(".content", tooltipConfig.content),
      ]);
    }
  }
}
