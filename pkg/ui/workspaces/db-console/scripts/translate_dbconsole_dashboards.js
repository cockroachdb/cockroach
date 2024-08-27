const fs = require('fs');
const path = require('path');
const { parse } = require('@typescript-eslint/typescript-estree');
const _ = require("lodash");
const React = require("react");

const translateMetricDef = (metricElement, i, groupByNode) => {
  const metricDef = {
    "query": {
      "data_source": "metrics",
      "name": "query" + (i+1)
    },
    "formula": {
      "alias": "",
      "formula": "query" + (i+1),
    },
  };
  metricElement.openingElement.attributes.forEach(attr => {
    // if the attribute is name, extract the metric
    if (attr.name.name === 'name') {
      let query = attr.value.expression ? attr.value.expression.value : attr.value.value;
      // Replace cr.node or cr.store with cockroachdb in the name
      query = query.replace('cr.node', 'cockroachdb');
      query = query.replace('cr.store', 'cockroachdb');
      query = query.replaceAll('-', '_');
      // This doesn't always work, since some metrics are manually named .count...
      // // Replace .count suffix
      // if (query.endsWith('.count')) {
      //   query = query.slice(0, -6);
      // }
      // regex capture the end if it's p and then a number, including decimals
      const regex = /_(p([0-9.]+)|max|min)$/;
      // Replace .p99 or max or min suffix
      const match = query.match(regex)
      if (match) {
        query = match[1] + ":" +query.replace(regex, '');
      }
      query = query + "{$cluster}";
      if (groupByNode) {
        query += ' by {node}';
      }
      metricDef.query.query = query;
    }
    if (attr.name.name === 'nonNegativeRate') {
      metricDef.query.query += '.as_rate()'
    }
    if (attr.name.name === 'title') {
      metricDef.formula.alias = attr.value.expression ? attr.value.expression.value : attr.value.value;
    }
  });
  return metricDef;
}

// Function to extract metrics
const extractGraph = (node) => {
  let def = {
    title: '',
    type: 'timeseries',
    requests: [
      {
        response_format: "timeseries",
        queries: [],
        formulas: [],
      }
    ]
  };
  if (node.type === 'JSXElement' && node.openingElement.name.name === 'LineGraph') {
    node.openingElement.attributes.forEach(attr => {
      if (attr.name.name === 'title') {
        def.title = attr.value.expression ? attr.value.expression.value : attr.value.value;
      }
    });
    // find Axis element
    for (let i = 0; i < node.children.length; i++) {
      let child = node.children[i];
      if (child.type === 'JSXElement' && child.openingElement.name.name === 'Axis') {
        // Find Metric elements, add them to definition
        if (child.children.length === 0) {
          continue
        }
        let groupingByNode = false;
        child.children.forEach(elt => {
          if (elt.type === 'JSXExpressionContainer') {
            // We're probably parsing an expression like this:
            //{_.map(nodeIDs, nid => (
            //  <Metric
            //    key={nid}
            //    name="cr.store.replicas"
            //    title={nodeDisplayName(nodeDisplayNameByID, nid)}
            //    sources={storeIDsForNode(storeIDsByNodeID, nid)}
            //  />
            //))}
            // We'll just skip into the Metric section, and add a group by by node.
            let listName;
            if (elt.expression.callee.name === "map") {
              // Lodash map
              child = elt.expression.arguments[1].body
              listName = elt.expression.arguments[0].name
            } else if (elt.expression.callee.name === "storeMetrics") {
              // Special case metrics type in storage.tsx.
              listName = "nodeIDs"
              //child = elt.expression.arguments[1].body
              parsed = parse("<Metric foo=\"bar\"/>", {jsx:true})
              child = parsed.body[0].expression
              child.openingElement.attributes = elt.expression.arguments[0].properties.map(prop => {
                return {
                  type: "JSXElement",
                  name: {type: "JSXIdentifier", name: prop.key.name},
                  value: prop.value,
                }
              })
            } else {
              // Native map
              child = elt.expression.arguments[0].body
              listName = elt.expression.callee.object.name
            }
            if (listName !== "nodeIDs") {
              console.error("can't parse, giving up", elt.expression.arguments[0])
              child = null
              // Just give up in this case - some people are trying to map
              // over percentiles.
              return
            }
            // It might be that we have an empty element with several children,
            // in which case we should be navigating to that empty element.
            // It would look like this: => (
            //  <>
            //    <Metric...>
            //    <Metric...>
            //    <Metric...>
            //  </>
            if (child.children.length > 0) {
              elt = child.children[0]
              //console.log(elt)
              if (elt.type === 'JSXElement' && elt.openingElement.name.name === '') {
                child = elt
              }
            } else {
              // Otherwise, put the 1 element into a list.
              child = {children: [child]}
            }
            groupingByNode = true;
          }
        });
        if (child == null) {
          continue
        }
        let x = 0;
        for (let j = 0; j < child.children.length; j++) {
          let elt = child.children[j];
          if (elt.type === 'JSXElement' && elt.openingElement.name.name === 'Metric') {
            const metricDef = translateMetricDef(elt, x, groupingByNode);
            x++;
            def.requests[0].queries.push(metricDef.query);
            def.requests[0].formulas.push(metricDef.formula);
          }
        }
      }
    }
  }
  return {
    definition: def,
  };
};

const generateGraphGroup = (filePath) => {
  const tsContent = fs.readFileSync(filePath, 'utf8');
  console.error("Processing file", filePath)

  // Parse the content to AST
  const ast = parse(tsContent, {
    loc: true,
    tokens: true,
    comment: true,
    jsx: true
  });

  let graphList = []
  outer: for (let i = 0; i < ast.body.length; i++) {
    let node = ast.body[i];
    // Find the export default statement
    if (node.type === 'ExportDefaultDeclaration') {
      // Find the return statement
      for (let j = 0; j < node.declaration.body.body.length; j++) {
        let child = node.declaration.body.body[j];
        if (child.type === 'ReturnStatement') {
          const returnArray = child.argument;
          // Add all the JSX elements to the graphList.
          for (let k = 0; k < returnArray.elements.length; k++) {
            if (returnArray.elements[k].type === 'JSXElement') {
              graphList.push(returnArray.elements[k]);
            }
          }
          break outer;
        }
      }
    }
  }


  return {
    definition: {
      title: path.basename(filePath).replace(".tsx", ""),
      type: "group",
      "layout_type": "ordered",
      widgets: graphList.map(graph => extractGraph(graph))
    }
  }
};

const processDirectory = (directoryPath) => {
  fs.readdir(directoryPath, (err, files) => {
    if (err) {
      console.error("Error reading directory:", err);
      return;
    }

    dashboard = {
      "title": "DB Console",
      "description": `This dashboard is automatically generated from the DB Console graph tsx definition files by
      the translate_dbconsole_dashboards.js script.`,
      "widgets": [],
      "layout_type": "ordered",
      "template_variables": [
        {
          "name": "cluster",
          "prefix": "cluster",
          "available_values": [],
          "default": "*"
        }
      ]
    }
    files.forEach(file => {
      if (path.extname(file) === '.tsx') {
        const filePath = path.join(directoryPath, file);
        const groupWidget = generateGraphGroup(filePath);
        if (groupWidget.definition.widgets.length > 0) {
          dashboard.widgets.push(groupWidget);
        }
      }
    });
    console.log(JSON.stringify(dashboard, null, 2));
  });
};

// Example usage
// node translate_dbconsole_dashboards.js /path/to/dbconsole/graphs > output.json
const directoryPath = process.argv[2]; // Pass the directory as a command-line argument
processDirectory(directoryPath);

