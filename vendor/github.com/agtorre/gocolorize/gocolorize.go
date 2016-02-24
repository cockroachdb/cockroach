package gocolorize

import (
	"fmt"
	"strings"
)

//internal usage
var plain = false

const (
	start             = "\033["
	reset             = "\033[0m"
	bold              = "1;"
	blink             = "5;"
	underline         = "4;"
	inverse           = "7;"
	normalIntensityFg = 30
	highIntensityFg   = 90
	normalIntensityBg = 40
	highIntensityBg   = 100
)

//The rest is external facing

//Color can be used for Fg and Bg
type Color int

const (
	ColorNone = iota
	//placeholder here so we don't confuse with ColorNone
	Red
	Green
	Yellow
	Blue
	Magenta
	Cyan
	White
	Black Color = -1
)

var colors = map[string]Color{
	"red":     Red,
	"green":   Green,
	"yellow":  Yellow,
	"blue":    Blue,
	"magenta": Magenta,
	"cyan":    Cyan,
	"white":   White,
	"black":   Black,
}

//Can set 1 or more of these properties
//This struct holds the state
type Property struct {
	Bold      bool
	Blink     bool
	Underline bool
	Inverse   bool
	Fgi       bool
	Bgi       bool
}

//Where the magic happens
type Colorize struct {
	Value []interface{}
	Fg    Color
	Bg    Color
	Prop  Property
}

//returns a value you can stick into print, of type string
func (c Colorize) Paint(v ...interface{}) string {
	c.Value = v
	return fmt.Sprint(c)
}

func propsString(p Property) string {
	var result string
	if p.Bold {
		result += bold
	}
	if p.Blink {
		result += blink
	}
	if p.Underline {
		result += underline
	}
	if p.Inverse {
		result += inverse
	}
	return result
}

// Format allows ColorText to satisfy the fmt.Formatter interface. The format
// behaviour is the same as for fmt.Print.
func (ct Colorize) Format(fs fmt.State, c rune) {
	var base int
	//fmt.Println(ct.Fg, ct.Fgi, ct.Bg, ct.Bgi, ct.Prop)
	//First Handle the Fg styles and options
	if ct.Fg != ColorNone && !plain {
		if ct.Prop.Fgi {
			base = int(highIntensityFg)
		} else {
			base = int(normalIntensityFg)
		}
		if ct.Fg == Black {
			base = base
		} else {
			base = base + int(ct.Fg)
		}
		fmt.Fprint(fs, start, "0;", propsString(ct.Prop), base, "m")
	}
	//Next Handle the Bg styles and options
	if ct.Bg != ColorNone && !plain {
		if ct.Prop.Bgi {
			base = int(highIntensityBg)
		} else {
			base = int(normalIntensityBg)
		}
		if ct.Bg == Black {
			base = base
		} else {
			base = base + int(ct.Bg)
		}
		//We still want to honor props if only the background is set
		if ct.Fg == ColorNone {
			fmt.Fprint(fs, start, propsString(ct.Prop), base, "m")
			//fmt.Fprint(fs, start, base, "m")
		} else {
			fmt.Fprint(fs, start, base, "m")
		}
	}

	// I simplified this to be a bit less efficient,
	// but more robust, it will work with anything that
	// printf("%v") will support
	for i, v := range ct.Value {
		fmt.Fprintf(fs, fmt.Sprint(v))
		if i < len(ct.Value)-1 {
			fmt.Fprintf(fs, " ")
		}
	}

	//after we finish go back to a clean state
	if !plain {
		fmt.Fprint(fs, reset)
	}
}

func NewColor(style string) Colorize {
	//Thank you https://github.com/mgutz/ansi for
	//this code example and for a bunch of other ideas
	foreground_background := strings.Split(style, ":")
	foreground := strings.Split(foreground_background[0], "+")
	fg := colors[foreground[0]]
	fgStyle := ""
	if len(foreground) > 1 {
		fgStyle = foreground[1]
	}

	var bg Color
	bgStyle := ""
	if len(foreground_background) > 1 {
		background := strings.Split(foreground_background[1], "+")
		bg = colors[background[0]]
		if len(background) > 1 {
			bgStyle = background[1]
		}
	}

	c := Colorize{Fg: fg, Bg: bg}
	if len(fgStyle) > 0 {
		if strings.Contains(fgStyle, "b") {
			c.ToggleBold()
		}
		if strings.Contains(fgStyle, "B") {
			c.ToggleBlink()
		}
		if strings.Contains(fgStyle, "u") {
			c.ToggleUnderline()
		}
		if strings.Contains(fgStyle, "i") {
			c.ToggleInverse()
		}
		if strings.Contains(fgStyle, "h") {
			c.ToggleFgIntensity()
		}
	}

	if len(bgStyle) > 0 {
		if strings.Contains(bgStyle, "h") {
			c.ToggleBgIntensity()
		}
	}
	return c
}

func (C *Colorize) SetColor(c Color) {
	C.Fg = c
}

func (C *Colorize) SetBgColor(b Color) {
	C.Bg = b
}

func (C *Colorize) ToggleFgIntensity() {
	C.Prop.Fgi = !C.Prop.Fgi
}

func (C *Colorize) ToggleBgIntensity() {
	C.Prop.Bgi = !C.Prop.Bgi
}

func (C *Colorize) ToggleBold() {
	C.Prop.Bold = !C.Prop.Bold
}

func (C *Colorize) ToggleBlink() {
	C.Prop.Blink = !C.Prop.Blink
}

func (C *Colorize) ToggleUnderline() {
	C.Prop.Underline = !C.Prop.Underline
}

func (C *Colorize) ToggleInverse() {
	C.Prop.Inverse = !C.Prop.Inverse
}

func SetPlain(p bool) {
	plain = p
}
