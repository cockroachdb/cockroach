// res_compat.c
#include <resolv.h>

// Provide a weak __res_nsearch for old krb5 objects.
// Forward to the public res_nsearch symbol on modern glibc.
__attribute__((weak))
int __res_nsearch(res_state statp,
                  const char *dname,
                  int class,
                  int type,
                  unsigned char *answer,
                  int anslen) {
  return res_nsearch(statp, dname, class, type, answer, anslen);
}
