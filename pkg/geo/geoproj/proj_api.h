/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Public (application) include file for PROJ.4 API, and constants.
 * Author:   Frank Warmerdam, <warmerdam@pobox.com>
 *
 ******************************************************************************
 * Copyright (c) 2001, Frank Warmerdam <warmerdam@pobox.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

/* General projections header file */
#ifndef PROJ_API_H
#define PROJ_API_H

/* standard inclusions */
#include <math.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * This version number should be updated with every release!  The format of
 * PJ_VERSION is
 *
 * * Before version 4.10.0: PJ_VERSION=MNP where M, N, and P are the major,
 *   minor, and patch numbers; e.g., PJ_VERSION=493 for version 4.9.3.
 *
 * * Version 4.10.0 and later: PJ_VERSION=MMMNNNPP later where MMM, NNN, PP
 *   are the major, minor, and patch numbers (the minor and patch numbers
 *   are padded with leading zeros if necessary); e.g., PJ_VERSION=401000
 *   for version 4.10.0.
 */
#define PJ_VERSION 493

/* pj_init() and similar functions can be used with a non-C locale */
/* Can be detected too at runtime if the symbol pj_atof exists */
#define PJ_LOCALE_SAFE 1

extern char const pj_release[]; /* global release id string */

#define RAD_TO_DEG	57.295779513082321
#define DEG_TO_RAD	.017453292519943296


extern int pj_errno;	/* global error return code */

#if !defined(PROJECTS_H)
    typedef struct { double u, v; } projUV;
    typedef struct { double u, v, w; } projUVW;
    typedef void *projPJ;
    #define projXY projUV
    #define projLP projUV
    #define projXYZ projUVW
    #define projLPZ projUVW
    typedef void *projCtx;
#else
    typedef PJ *projPJ;
    typedef projCtx_t *projCtx;
#   define projXY	XY
#   define projLP       LP
#   define projXYZ      XYZ
#   define projLPZ      LPZ
#endif

/* file reading api, like stdio */
typedef int *PAFile;
typedef struct projFileAPI_t {
    PAFile  (*FOpen)(projCtx ctx, const char *filename, const char *access);
    size_t  (*FRead)(void *buffer, size_t size, size_t nmemb, PAFile file);
    int     (*FSeek)(PAFile file, long offset, int whence);
    long    (*FTell)(PAFile file);
    void    (*FClose)(PAFile);
} projFileAPI;

/* procedure prototypes */

projXY pj_fwd(projLP, projPJ);
projLP pj_inv(projXY, projPJ);

projXYZ pj_fwd3d(projLPZ, projPJ);
projLPZ pj_inv3d(projXYZ, projPJ);

int pj_transform( projPJ src, projPJ dst, long point_count, int point_offset,
                  double *x, double *y, double *z );
int pj_datum_transform( projPJ src, projPJ dst, long point_count, int point_offset,
                        double *x, double *y, double *z );
int pj_geocentric_to_geodetic( double a, double es,
                               long point_count, int point_offset,
                               double *x, double *y, double *z );
int pj_geodetic_to_geocentric( double a, double es,
                               long point_count, int point_offset,
                               double *x, double *y, double *z );
int pj_compare_datums( projPJ srcdefn, projPJ dstdefn );
int pj_apply_gridshift( projCtx, const char *, int,
                        long point_count, int point_offset,
                        double *x, double *y, double *z );
void pj_deallocate_grids(void);
void pj_clear_initcache(void);
int pj_is_latlong(projPJ);
int pj_is_geocent(projPJ);
void pj_get_spheroid_defn(projPJ defn, double *major_axis, double *eccentricity_squared);
void pj_pr_list(projPJ);
void pj_free(projPJ);
void pj_set_finder( const char *(*)(const char *) );
void pj_set_searchpath ( int count, const char **path );
projPJ pj_init(int, char **);
projPJ pj_init_plus(const char *);
projPJ pj_init_ctx( projCtx, int, char ** );
projPJ pj_init_plus_ctx( projCtx, const char * );
char *pj_get_def(projPJ, int);
projPJ pj_latlong_from_proj( projPJ );
void *pj_malloc(size_t);
void pj_dalloc(void *);
void *pj_calloc (size_t n, size_t size);
void *pj_dealloc (void *ptr);
char *pj_strerrno(int);
int *pj_get_errno_ref(void);
const char *pj_get_release(void);
void pj_acquire_lock(void);
void pj_release_lock(void);
void pj_cleanup_lock(void);

projCtx pj_get_default_ctx(void);
projCtx pj_get_ctx( projPJ );
void pj_set_ctx( projPJ, projCtx );
projCtx pj_ctx_alloc(void);
void    pj_ctx_free( projCtx );
int pj_ctx_get_errno( projCtx );
void pj_ctx_set_errno( projCtx, int );
void pj_ctx_set_debug( projCtx, int );
void pj_ctx_set_logger( projCtx, void (*)(void *, int, const char *) );
void pj_ctx_set_app_data( projCtx, void * );
void *pj_ctx_get_app_data( projCtx );
void pj_ctx_set_fileapi( projCtx, projFileAPI *);
projFileAPI *pj_ctx_get_fileapi( projCtx );

void pj_log( projCtx ctx, int level, const char *fmt, ... );
void pj_stderr_logger( void *, int, const char * );

/* file api */
projFileAPI *pj_get_default_fileapi();

PAFile pj_ctx_fopen(projCtx ctx, const char *filename, const char *access);
size_t pj_ctx_fread(projCtx ctx, void *buffer, size_t size, size_t nmemb, PAFile file);
int    pj_ctx_fseek(projCtx ctx, PAFile file, long offset, int whence);
long   pj_ctx_ftell(projCtx ctx, PAFile file);
void   pj_ctx_fclose(projCtx ctx, PAFile file);
char  *pj_ctx_fgets(projCtx ctx, char *line, int size, PAFile file);

PAFile pj_open_lib(projCtx, const char *, const char *);

int pj_run_selftests (int verbosity);


#define PJ_LOG_NONE        0
#define PJ_LOG_ERROR       1
#define PJ_LOG_DEBUG_MAJOR 2
#define PJ_LOG_DEBUG_MINOR 3

#ifdef __cplusplus
}
#endif

#endif /* ndef PROJ_API_H */

