'use strict';

// imports
var gulp = require('gulp')
    ,stylus = require('gulp-stylus')
    ,nib = require('nib')
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

// watch files for changes
gulp.task('watch', function () {
    livereload.listen();

    gulp.watch(['styl/**/*.styl', 'styl/**/*.css'], ['stylus']);
});

// default task is watch
gulp.task('default', ['watch']);
