'use strict';

// imports
var gulp = require('gulp')
    ,stylus = require('gulp-stylus')
    ,nib = require('nib')
    ,ts = require('gulp-typescript')
    ,livereload = require('gulp-livereload')
    ,del = require('del')
    ,rename = require('gulp-rename')
    ;

// generate css from styl files
gulp.task('stylus', function () {
    return gulp.src('styl/app.styl')
        .pipe(
          stylus({
            "include css": true,
            use: nib(),
          })
        )
        .pipe(rename('app_debug.css'))
        .pipe(gulp.dest('build'))
        .pipe(livereload());
});

//typescript
var tsProject = ts.createProject('./ts/tsconfig.json', {outFile: './build/app.js'});
gulp.task('typescript', function () {
    return tsProject.src()
        .pipe(ts(tsProject))
        .js
        .pipe(gulp.dest('.'))
        .pipe(livereload());
});

gulp.task('deleteIndex', function () {
   return del('index.html');
});

// copy index.html
gulp.task('copyIndex', ['deleteIndex'], function () {
    return gulp.src('debug/index.html')
        .pipe(gulp.dest('./', {mode: '0444'}))
        .pipe(livereload());
});

// watch files for changes
gulp.task('watch', function () {

    livereload.listen();

    gulp.watch(['styl/**/*.styl', 'styl/**/*.css'], ['stylus']);
    gulp.watch('ts/**/*.ts', ['typescript']);
    gulp.watch('debug/index.html', ['copyIndex']);
});

// default task is watch
gulp.task('default', ['watch']);
