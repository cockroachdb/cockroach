#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const d3geo = require('d3-geo');
const d3geoProjection = require('d3-geo-projection');
const topojson = require('topojson');

const us_orig = require('../node_modules/us-atlas/us/10m.json');
const world_orig = require('../node_modules/world-atlas/world/50m.json');

const projection = d3geo.geoAlbersUsa().scale(1280).translate([480, 300]);
const invert = d3geo.geoTransform({
    point: function(x, y) {
        const inverted = projection.invert([x, y]);
        this.stream.point(inverted[0], inverted[1]);
    }
});

const us_feat_projected = topojson.feature(us_orig, {
    type: "GeometryCollection",
    geometries: us_orig.objects.states.geometries,//.filter(s => s.id != "02"),
});
const us_feat = d3geoProjection.geoProject(us_feat_projected, invert);
const world_feat = topojson.feature(world_orig, {
    type: "GeometryCollection",
    geometries: world_orig.objects.countries.geometries.filter(c => c.id != "840"),
});

const combined_feats = {
    type: "FeatureCollection",
    features: us_feat.features.concat(world_feat.features),
};

const combined_topo = topojson.topology({all: combined_feats}, 1e3);

let combined_simpl = topojson.presimplify(combined_topo);
combined_simpl = topojson.simplify(combined_simpl, 0.25);

const combined_result = topojson.feature(combined_simpl, combined_simpl.objects.all);
const combined_serialized = JSON.stringify(combined_result);

const outfile = path.join(__dirname, '..', 'ccl', 'src', 'views', 'clusterviz', 'containers', 'map', 'world.json');
fs.writeFileSync(outfile, combined_serialized);
