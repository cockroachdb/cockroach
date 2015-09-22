'use strict';

// Initially copied from: https://www.npmjs.com/package/generator-gulp-mithril

var gulp = require('gulp'),
    stylus = require('gulp-stylus'),
    typescript = require('gulp-typescript')
    ;


/* styles */
gulp.task('styles', function () {
    return gulp.src('styl/app.styl')
        .pipe(stylus({
            compress: true
        }))
        .pipe(gulp.dest('css'));

});

/* typescript */
gulp.task('typescript', function () {
    return gulp.src(['ts/app.ts', 'ts/header.ts'])
        .pipe(typescript(require('./ts/tsconfig.json').compilerOptions))
        .pipe(gulp.dest('js/app.js'));

});

/* copy bower, tsd components */
gulp.task('bower', function () {
    var paths = {
        js: [
            'bower_components/d3/d3.min.js',
            'bower_components/mithril/mithril.min.js',
            'bower_components/lodash/lodash.js',
            'bower_components/nvd3/build/nv.d3.min.js'
        ],
        css: [
            'bower_components/nvd3/build/nv.d3.min.css'
        ],
        typings: [
            'typings/*'
        ]
    };

    gulp.src(paths.js)
        .pipe(gulp.dest('js/libs'));

    gulp.src(paths.css)
        .pipe(gulp.dest('css/libs'));

    gulp.src(paths.typings)
        .pipe(gulp.dest('ts/typings'));
});

/* watch */
gulp.task('watch', ['bower'], function () {

    gulp.watch('styl/**/*.styl', ['styles']);

    gulp.watch('ts/**/*.ts', ['typescript']);
});

/* build */
gulp.task('build', ['styles', 'bower', 'typescript']);

/* default */
gulp.task('default', function () {
    gulp.start('watch');
});