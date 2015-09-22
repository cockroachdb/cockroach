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
        .pipe(gulp.dest('build/css'));

});

var paths = {
    js: [
        'bower_components/d3/d3.min.js',
        'bower_components/mithril/mithril.min.js',
        'bower_components/lodash/lodash.js',
        'bower_components/nvd3/build/nv.d3.min.js'
    ],
    css: [
        'bower_components/nvd3/build/nv.d3.min.css'
    ]
};

/* copy bower js libs */
gulp.task('bowerjs', function () {
    return gulp.src(paths.js)
        .pipe(gulp.dest('build/js/libs'));
});

/* copy bower css libs */
gulp.task('bowercss', function () {
    return gulp.src(paths.css)
        .pipe(gulp.dest('build/css/libs'));
});

gulp.task('bower', ['bowerjs', 'bowercss']);

/* typescript */
gulp.task('typescript', function () {
    return gulp.src(['ts/app.ts', 'ts/header.ts'])
        .pipe(typescript(require('./ts/tsconfig.json').compilerOptions))
        .pipe(gulp.dest('build/js'));
});

/* copy index */
gulp.task('copyindex', function () {
    return gulp.src('index.html')
        .pipe(gulp.dest('build'));
});

/* build */
gulp.task('build', ['styles', 'bower', 'copyindex', 'typescript']);

/* watch */
gulp.task('watch', ['build'], function () {

    gulp.watch('styl/**/*.styl', ['styles']);

    gulp.watch('ts/**/*.ts', ['typescript']);

    gulp.watch('index.html', ['copyindex']);

});

/* default */
gulp.task('default', function () {
    gulp.start('watch');
});