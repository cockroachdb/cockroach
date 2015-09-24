// Package metadata for Meteor.js full stack web framework
// This file is defined in Meteor documentation at http://docs.meteor.com/#/full/packagejs
// and used by Meteor https://www.meteor.com/ and its package repository Atmosphere https://atmospherejs.com

Package.describe({
    "name": 'nvd3:nvd3',
    summary: 'Nvd3.org charts.',
    version: '1.8.1',
    git: "https://github.com/novus/nvd3.git"
});
Package.on_use(function (api) {
    api.versionsFrom("METEOR@1.0");
    api.use('d3js:d3@3.5.5', 'client');
    api.add_files('build/nv.d3.js', 'client');
    api.add_files('build/nv.d3.css', 'client');
    api.export("nv");
});