'use strict';

// Initially copied from: https://www.npmjs.com/package/generator-gulp-mithril

var gulp = require('gulp'),
    stylus = require('gulp-stylus'),
    typescript = require('gulp-typescript'),
    bindata = require('gulp-gobin'),
    del = require('del'),
    shell = require('gulp-shell')
    ;


gulp.task('clean:styles', function (cb) {
    return del('build/css/**');
});

gulp.task('clean:js', function (cb) {
    return del('build/js/**');
});

gulp.task('clean:index', function (cb) {
    return del('build/index.html');
});

/* copy bower js libs */
gulp.task('bowerjs', ['clean:js'], function () {
    return gulp.src(paths.js)
        .pipe(gulp.dest('build/js/libs'));
});

/* copy bower css libs */
gulp.task('bowercss', ['clean:styles'], function () {
    return gulp.src(paths.css)
        .pipe(gulp.dest('build/css/libs'));
});

/* styles */
gulp.task('styles', ['clean:styles', 'bowercss'], function () {
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
        'bower_components/lodash/lodash.min.js',
        'bower_components/nvd3/build/nv.d3.min.js'
    ],
    css: [
        'bower_components/nvd3/build/nv.d3.min.css'
    ]
};

gulp.task('bower', ['bowerjs', 'bowercss']);

/* typescript */
gulp.task('typescript', ['clean:js', 'bowerjs'], function () {
    return gulp.src(['ts/app.ts', 'ts/header.ts'])
        .pipe(typescript(require('./ts/tsconfig.json').compilerOptions))
        .pipe(gulp.dest('build'));
});

/* copy index */
gulp.task('copyindex', ['clean:index'], function () {
    return gulp.src('index.html')
        .pipe(gulp.dest('build'));
});

gulp.task('clean:embedded', function () {
    return del('embedded.go');
});

gulp.task('bindata:dist', ['clean:embedded'], shell.task([
    'go-bindata -mode 0644 -modtime 1400000000 -pkg ui -o embedded.go build/...',
    'gofmt -s -w embedded.go',
    'goimports -w embedded.go'
]));

gulp.task('bindata:debug', ['clean:embedded'], shell.task([
    'go-bindata -pkg ui -o embedded.go -debug build/...'
]));

/* build */
gulp.task('build', ['styles', 'typescript', 'copyindex'], function(cb) {
    cb();
});

/* compile */
gulp.task('compile', ['build'], function () {
    return gulp.start('bindata:dist')
});

/* debug */
gulp.task('debug', ['build'], function () {
    return gulp.start('bindata:debug')
});
/* watch */
gulp.task('watch', ['build'], function () {
    gulp.watch('styl/**/*.styl', ['styles', 'bindata']);

    gulp.watch('ts/**/*.ts', ['typescript', 'bindata']);

    gulp.watch('index.html', ['copyindex', 'bindata']);
});

/* default */
gulp.task('default', function () {
    return gulp.start('watch');
});