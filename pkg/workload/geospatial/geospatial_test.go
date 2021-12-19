package geospatial

import (
	"fmt"
	"os"
	"testing"

	"gopkg.in/yaml.v2"
)

type myFloat float64

func (f myFloat) MarshalYAML() (interface{}, error) {
	if float64(f) == float64(int64(f)) {
		// Special case; add a .0 to whole numbers so they decode to float.
		return fmt.Sprintf("FOO%d.0FOO", int64(f)), nil
	}
	return float64(f), nil
}

func TestDumpData(t *testing.T) {
	for _, x := range []struct {
		name   string
		object [][]interface{}
	}{
		{"nyc_census_blocks", nycCensusBlocksRows[:]},
		{"nyc_homicides", nycHomicidesRows[:]},
		{"nyc_neighborhoods", nycNeighborhoodsRows[:]},
		{"nyc_streets", nycStreetsRows[:]},
		{"nyc_subway_stations", nycSubwayStationsRows[:]},
		{"subway_lines", subwayLinesRows[:]},
	} {

		for _, o := range x.object {
			for j := range o {
				f, ok := o[j].(float64)
				if ok {
					o[j] = myFloat(f)
				}
			}
		}

		data, err := yaml.Marshal(x.object)
		if err != nil {
			t.Fatal(err)
		}
		filename := fmt.Sprintf("data/%s.yaml", x.name)
		fi, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0660)
		if err != nil {
			t.Fatal(err)
		}
		//w, err := gzip.NewWriterLevel(fi, gzip.BestCompression)
		//if err != nil {
		//	t.Fatal(err)
		//}
		fi.Write(data)
		//w.Close()
		fi.Close()
	}
}
