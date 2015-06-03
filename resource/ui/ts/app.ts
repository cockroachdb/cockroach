// source: app.ts
/// <reference path="typings/mithriljs/mithril.d.ts" />

/// <reference path="pages/graph.ts" />
/// <reference path="pages/monitor.ts" />
/// <reference path="pages/nodes.ts" />
/// <reference path="pages/rest_explorer.ts" />
/// <reference path="pages/stores.ts" />

// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

m.route.mode = "hash";
m.route(document.getElementById("root"), "/rest-explorer", {
    "/graph": AdminViews.Graph.Page,
    "/monitor": AdminViews.Monitor.Page,
    "/node": AdminViews.Nodes.NodesPage,
    "/nodes": AdminViews.Nodes.NodesPage,
    "/node/:node_id": AdminViews.Nodes.NodePage,
    "/nodes/:node_id": AdminViews.Nodes.NodePage,
    "/rest-explorer": AdminViews.RestExplorer.Page,
    "/store": AdminViews.Stores.StorePage,
    "/stores": AdminViews.Stores.StoresPage,
    "/store/:store_id": AdminViews.Stores.StorePage,
    "/stores/:store_id": AdminViews.Stores.StorePage
});
