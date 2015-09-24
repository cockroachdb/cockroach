'use strict';

// imports
var del = require('del'),
    gulp = require('gulp')
    ;

// gulp modules
var stylus = require('gulp-stylus'),
    ts = require('gulp-typescript'),
    shell = require('gulp-shell')
    ;

// clean build files
gulp.task('clean:styles', function (cb) {
    return del('build/css/app.css');
});

gulp.task('clean:js', function (cb) {
    return del('build/js/app.js');
});

gulp.task('clean:bowerstyles', function (cb) {
    return del('build/css/lib/**');
});

gulp.task('clean:bowerjs', function (cb) {
    return del('build/js/lib/**');
});

// clean generated go file
gulp.task('clean:embedded', function () {
    return del('embedded.go');
});

gulp.task('clean:index', function (cb) {
    return del('build/index.html');
});

// copy over bower dependencies
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

gulp.task('bowerjs', ['clean:bowerjs'], function () {
    return gulp.src(paths.js)
        .pipe(gulp.dest('build/js/libs'));
});

gulp.task('bowercss', ['clean:bowerstyles'], function () {
    return gulp.src(paths.css)
        .pipe(gulp.dest('build/css/libs'));
});

gulp.task('bower', ['bowerjs', 'bowercss']);

// generate css from styl files
gulp.task('styles', ['clean:styles'], function () {
    return gulp.src('styl/app.styl')
        .pipe(stylus({
            compress: true
        }))
        .pipe(gulp.dest('build/css'));

});

// generate js from typescript
var tsProject = ts.createProject('./ts/tsconfig.json');
gulp.task('typescript', ['clean:js'], function () {
    return tsProject.src()
        .pipe(ts(tsProject))
        .js
        .pipe(gulp.dest('build'));
});

// copy index.html
gulp.task('copyindex', ['clean:index'], function () {
    return gulp.src('index.html')
        .pipe(gulp.dest('build'));
});

// generate all frontend files
gulp.task('build', ['styles', 'typescript', 'copyindex', 'bower']);

// generate embedded go file
gulp.task('bindata:dist', ['clean:embedded', 'build'], shell.task([
    'go-bindata -mode 0644 -modtime 1400000000 -pkg ui -o embedded.go build/...',
    'gofmt -s -w embedded.go',
    'goimports -w embedded.go'
]));

// generate embedded go file for debugging (passes through to build folder)
gulp.task('bindata:debug', ['clean:embedded', 'build'], shell.task([
    'go-bindata -pkg ui -o embedded.go -debug build/...'
]));

//convenience tasks for generating debug/dist versions of the embedded.go file
gulp.task('debug', ['bindata:debug']);
gulp.task('dist', ['bindata:dist']);

// watch files for changes
gulp.task('watch', ['debug'], function () {
    gulp.watch('styl/**/*.styl', ['styles']);
    gulp.watch('ts/**/*.ts', ['typescript']);
    gulp.watch('index.html', ['copyindex']);
});

// default task is watch
gulp.task('default', ['watch']);