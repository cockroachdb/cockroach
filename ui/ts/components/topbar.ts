// source: components/topbar.ts

/// <reference path="./health.ts" />
/// <reference path="../../typings/browser.d.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";

  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  export module Topbar {
    export function controller(): any {
      return {
        updatedStr: "-",
      };
    }
    export function view(ctrl: any, args: {title?: string|MithrilVirtualElement; updated: number; titleImage?: MithrilVirtualElement; }): MithrilVirtualElement {
      if (args.updated && _.isFinite(args.updated)) {
        ctrl.updatedStr = Utils.Format.Date(new Date(Utils.Convert.NanoToMilli(args.updated)));
      }

      return m(".topbar", [
        (args.title && m("h2", args.title) || (args.titleImage)),
        m(".info-container", [
          m(".health", [m("strong", "health:"), m.component(Components.Health, {})]),
          m(".last-updated", [ m("strong", "Updated: "), ctrl.updatedStr ]),
        ]),
      ]);
    }
  }

}
