#ifndef BSDSHIM_H
#define BSDSHIM_H

#include <ctype.h>
#include <string.h>
#include <time.h>

#define locale_t int

#ifdef isspace_l
#undef isspace_l
#endif
#define isspace_l(A, _) isspace(A)
#ifdef isdigit_l
#undef isdigit_l
#endif
#define isdigit_l(A, _) isdigit(A)
#ifdef isupper_l
#undef isupper_l
#endif
#define isupper_l(A, _) isupper(A)
#ifdef strtol_l
#undef strtol_l
#endif
#define strtol_l(A, B, C, _) strtol(A, B, C)

#ifdef isleap
#undef isleap
#endif
#define isleap(y)   (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

#define YEARSPERREPEAT		400	/* years before a Gregorian repeat */

#define SECSPERMIN	60
#define MINSPERHOUR	60
#define HOURSPERDAY	24
#define DAYSPERWEEK	7
#define DAYSPERNYEAR	365
#define DAYSPERLYEAR	366
#define SECSPERHOUR	(SECSPERMIN * MINSPERHOUR)
#define SECSPERDAY	(SECSPERHOUR * HOURSPERDAY)
#define MONSPERYEAR	12

#define TM_SUNDAY	0
#define TM_MONDAY	1
#define TM_TUESDAY	2
#define TM_WEDNESDAY	3
#define TM_THURSDAY	4
#define TM_FRIDAY	5
#define TM_SATURDAY	6

#define TM_JANUARY	0
#define TM_FEBRUARY	1
#define TM_MARCH	2
#define TM_APRIL	3
#define TM_MAY		4
#define TM_JUNE		5
#define TM_JULY		6
#define TM_AUGUST	7
#define TM_SEPTEMBER	8
#define TM_OCTOBER	9
#define TM_NOVEMBER	10
#define TM_DECEMBER	11

#define TM_YEAR_BASE	1900

#define EPOCH_YEAR	1970
#define EPOCH_WDAY	TM_THURSDAY

struct mytm {
        int     tm_sec;         /* seconds after the minute [0-60] */
        int     tm_min;         /* minutes after the hour [0-59] */
        int     tm_hour;        /* hours since midnight [0-23] */
        int     tm_mday;        /* day of the month [1-31] */
        int     tm_mon;         /* months since January [0-11] */
        int     tm_year;        /* years since 1900 */
        int     tm_wday;        /* days since Sunday [0-6] */
        int     tm_yday;        /* days since January 1 [0-365] */
        int     tm_isdst;       /* Daylight Savings Time flag */
        long    tm_gmtoff;      /* offset from UTC in seconds */
        char    *tm_zone;       /* timezone abbreviation */
        long    tm_nsec;        /* nanoseconds */
};

static int
mystrcasecmp(const char *s1, const char *s2)
{
        const unsigned char
	    *us1 = (const unsigned char *)s1,
	    *us2 = (const unsigned char *)s2;

        while (tolower(*us1) == tolower(*us2++))
                if (*us1++ == '\0')
                        return (0);
        return (tolower(*us1) - tolower(*--us2));
}

#ifdef strcasecmp_l
#undef strcasecmp_l
#endif
#define strcasecmp_l(A, B, _) mystrcasecmp(A, B)

static int
mystrncasecmp(const char *s1, const char *s2, size_t n)
{
        if (n != 0) {
                const unsigned char
                                *us1 = (const unsigned char *)s1,
                                *us2 = (const unsigned char *)s2;

                do {
                        if (tolower(*us1) != tolower(*us2++))
                                return (tolower(*us1) - tolower(*--us2));
                        if (*us1++ == '\0')
                                break;
                } while (--n != 0);
        }
        return (0);
}
#ifdef strncasecmp_l
#undef strncasecmp_l
#endif
#define strncasecmp_l(A, B, C, _) mystrncasecmp(A, B, C)

#ifdef gmtime_r
#undef gmtime_r
#endif
extern struct mytm* mygmtime_r(const time_t* timep, struct mytm* tm);
#define gmtime_r(A, B) mygmtime_r(A, B)


#endif
