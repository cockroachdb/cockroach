// source: component/tooltip.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />

module Components {
  "use strict";

  /**
   * Tooltip is a component that creates a configurable tooltip
   */
  export module Tooltip {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    interface TooltipConfig {
      // Class applied to the tooltip
      tooltipClass?: string;
      title?: string;
      content: (string|MithrilVirtualElement|MithrilVirtualElement[]|(MithrilVirtualElement|string)[]);
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
