// source: pages/nodes.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/navbar.ts" />
/// <reference path="../util/property.ts" />

// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  /**
   * SubModules contains distinct functional submodules used to build a page.
   */
  export module SubModules {
    /**
     * TitleBar is the bar across the top of the page.
     */
    export module TitleBar {
      import NavigationBar = Components.NavigationBar;

      class Controller {
        private static defaultTargets: NavigationBar.Target[] = [
          {
            title: "Nodes",
            route: "/nodes"
          },
          {
            title: "Stores",
            route: "/stores"
          }
        ];

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          let currentRoute = m.route();
          return _.startsWith(currentRoute, t.route);
        };

        public TargetSet(): NavigationBar.TargetSet {
          return {
            targets: Utils.Prop(Controller.defaultTargets),
            isActive: Controller.isActive
          };
        };
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        return m("header", m.component(NavigationBar, ctrl.TargetSet()));
      }
    }
  }
}
