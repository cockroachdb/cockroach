// source: components/topbar.ts

/// <reference path="./health.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";

  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  export module Topbar {
    export function controller(): void {}
    export function view(ctrl: any, args: {title?: string; updated: number; titleImage?: MithrilVirtualElement; }): MithrilVirtualElement {
      let updatedStr: string = "-";
      if (args.updated !== 0 && args.updated !== -Infinity) {
        updatedStr = Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(args.updated)));
      }

      return m(".topbar", [
        (args.title && m("h2", args.title) || (args.titleImage)),
        m(".info-container", [
          m(".health", [m("strong", "health:"), m.component(Components.Health, {})]),
          m(".last-updated", [ m("strong", "Updated: "), updatedStr ]),
        ]),
      ]);
    }
  }

}
