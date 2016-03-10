// source: pages/nodes.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../models/status.ts" />
/// <reference path="../components/metrics.ts" />
/// <reference path="../components/navbar.ts" />
/// <reference path="../util/property.ts" />
/// <reference path="../svgIcons/icons.ts" />

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
            title: "",
            route: "/",
            icon: SvgIcons.cockroachIcon,
            liClass: "cockroach",
          },
          {
            title: "Cluster",
            route: "/cluster",
            icon: SvgIcons.clusterIcon,
          },
          {
            title: "Nodes",
            route: "/nodes",
            icon: SvgIcons.nodesIcon,
          },
          {
            title: "Databases",
            route: "/databases",
            icon: SvgIcons.databaseIcon,
          },
          {
            title: "Help Us",
            route: "/help-us/reporting",
            icon: SvgIcons.cockroachIconSmall,
          },
        ].map(function(v: {title: string; route: string; icon: string; liClass?: string; }): NavigationBar.Target {
          return {
            view: [
              m(".image-container", m.trust(v.icon)),
              m("div", v.title),
            ],
            route: v.route,
            liClass: v.liClass,
          };
        });

        private static isActive: (targ: NavigationBar.Target) => boolean = (t: NavigationBar.Target) => {
          let currentRoute = m.route();
          return _.startsWith(currentRoute, t.route);
        };

        public TargetSet(): NavigationBar.TargetSet {
          return {
            baseRoute: "",
            targets: Utils.Prop(Controller.defaultTargets),
            isActive: Controller.isActive,
          };
        };
      }

      export function controller(): Controller {
        return new Controller();
      }

      export function view(ctrl: Controller): _mithril.MithrilVirtualElement {
        return m(
          "header",
          m.component(
            NavigationBar,
            {
              ts: ctrl.TargetSet(),
            }
          )
        );
      }
    }
  }
}
