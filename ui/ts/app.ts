// source: app.ts
/// <reference path="../bower_components/mithriljs/mithril.d.ts" />

/// <reference path="pages/navigation.ts" />
/// <reference path="pages/log.ts" />
/// <reference path="pages/cluster.ts" />
/// <reference path="pages/nodes.ts" />
/// <reference path="pages/helpus.ts" />
/// <reference path="pages/events.ts" />
/// <reference path="pages/databases.ts" />
/// <reference path="pages/banner.ts" />
/// <reference path="pages/footer.ts" />

m.mount(document.getElementById("header"), AdminViews.SubModules.TitleBar);
m.mount(document.getElementById("footer"), AdminViews.SubModules.Footer);

m.route.mode = "hash";
m.route(document.getElementById("root"), "/cluster", {
  "/nodes": AdminViews.Nodes.NodeOverviewReroute,
  "/nodes/overview": AdminViews.Nodes.NodesPage,
  "/nodes/events": AdminViews.Events.Page,
  "/nodes/graphs": AdminViews.Nodes.NodesPage,
  "/nodes/:node_id": AdminViews.Nodes.NodePage,
  "/nodes/:node_id/logs": AdminViews.Log.Page,
  "/nodes/:node_id/:detail": AdminViews.Nodes.NodePage,
  "/databases": AdminViews.Databases.DatabaseListPage,
  "/databases/:database": AdminViews.Databases.DatabasePage,
  "/databases/:database/tables/:table": AdminViews.Databases.TablePage,
  "/help-us/:detail": AdminViews.HelpUs.Page,
  "/cluster": AdminViews.Cluster.Page,
  "/cluster/:detail": AdminViews.Cluster.Page,
});

m.mount(document.getElementById("banner"), AdminViews.SubModules.Banner);
