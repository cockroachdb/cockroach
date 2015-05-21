// source: app.ts
/// <reference path="typings/mithriljs/mithril.d.ts" />
/// <reference path="pages/rest_explorer.ts" />
/// <reference path="pages/monitor.ts" />
/// <reference path="pages/graph.ts" />

// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Bram Gruneir (bramgruneir@gmail.com)

m.route.mode = "hash";
m.route(document.getElementById("root"), "/rest-explorer", {
    "/rest-explorer": AdminViews.RestExplorer.Page,
    "/monitor": AdminViews.Monitor.Page,
    "/graph": AdminViews.Graph.Page,
});
