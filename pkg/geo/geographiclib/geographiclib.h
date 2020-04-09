#if defined(__cplusplus)
extern "C" {
#endif

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
);

#if defined(__cplusplus)
}
#endif
