# go-kml

[![PkgGoDev](https://pkg.go.dev/badge/github.com/twpayne/go-kml)](https://pkg.go.dev/github.com/twpayne/go-kml)

Package `kml` provides convenience methods for creating and writing KML documents.

## Key Features

* Simple API for building arbitrarily complex KML documents.
* Support for all KML elements, including Google Earth `gx:` extensions.
* Compatibilty with the standard library [`encoding/xml`](https://pkg.go.dev/encoding/xml) package.
* Pretty (neatly indented) and compact (minimum size) output formats.
* Support for shared `Style` and `StyleMap` elements.
* Simple mapping between functions and KML elements.
* Convenience functions for using standard KML icons.
* Convenience functions for spherical geometry.

## Example

```go
func ExampleKML() {
    k := kml.KML(
        kml.Placemark(
            kml.Name("Simple placemark"),
            kml.Description("Attached to the ground. Intelligently places itself at the height of the underlying terrain."),
            kml.Point(
                kml.Coordinates(kml.Coordinate{Lon: -122.0822035425683, Lat: 37.42228990140251}),
            ),
        ),
    )
    if err := k.WriteIndent(os.Stdout, "", "  "); err != nil {
        log.Fatal(err)
    }
}
```

Output:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Placemark>
    <name>Simple placemark</name>
    <description>Attached to the ground. Intelligently places itself at the height of the underlying terrain.</description>
    <Point>
      <coordinates>-122.0822035425683,37.42228990140251</coordinates>
    </Point>
  </Placemark>
</kml>
```

There are more [examples in the
documentation](https://pkg.go.dev/github.com/twpayne/go-kml#pkg-examples)
corresponding to the [examples in the KML
tutorial](https://developers.google.com/kml/documentation/kml_tut).

## Subpackages

* [`icon`](https://pkg.go.dev/github.com/twpayne/go-kml/icon) Convenience functions for using standard KML icons.
* [`sphere`](https://pkg.go.dev/github.com/twpayne/go-kml/sphere) Convenience functions for spherical geometry.

## License

MIT
