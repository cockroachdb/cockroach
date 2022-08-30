/*-
 * Copyright (c) 1997-2002 FreeBSD Project.
 * All rights reserved.
 *
 * Copyright (c) 2011 The FreeBSD Foundation
 * All rights reserved.
 * Portions of this software were developed by David Chisnall
 * under sponsorship from the FreeBSD Foundation.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $FreeBSD: stable/10/lib/libc/stdtime/timelocal.h 227753 2011-11-20 14:45:42Z theraven $
 */

#ifndef _TIMELOCAL_H_
#define	_TIMELOCAL_H_
// #include "xlocale_private.h"
// #include <locale.h>
#include "bsdshim.h"

/*
 * Private header file for the strftime and strptime localization
 * stuff.
 */
struct lc_time_T {
	const char	*mon[12];
	const char	*month[12];
	const char	*wday[7];
	const char	*weekday[7];
	const char	*X_fmt;
	const char	*x_fmt;
	const char	*c_fmt;
	const char	*am;
	const char	*pm;
	const char	*date_fmt;
	const char	*alt_month[12];
	const char	*md_order;
	const char	*ampm_fmt;
};

extern const struct lc_time_T	_C_time_locale;

#define __get_current_time_locale(X) (&_C_time_locale)
// int	__time_load_locale(const char *);

#endif /* !_TIMELOCAL_H_ */
