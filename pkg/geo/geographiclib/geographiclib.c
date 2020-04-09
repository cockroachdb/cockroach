#include "geodesic.h"
#include "geographiclib.h"

void CR_GEOGRAPHICLIB_Inverse(
  double radius,
  double flattening,
  double aLat,
  double aLng,
  double bLat,
  double bLng,
  double *s12,
  double *az1,
  double *az2
) {
  struct geod_geodesic spheroid;
  geod_init(&spheroid, radius, flattening);
  geod_inverse(&spheroid, aLat, aLng, bLat, bLng, s12, az1, az2);
}
