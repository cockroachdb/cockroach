// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sparkline

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/png"
	"io"
)

// ITermImage is a struct that holds an image.Image, and implements the io.WriterTo interface.
// The implementation encodes an image as the corresponding iTerm-escaped image string [1].
// [1] https://iterm2.com/documentation-images.html
type ITermImage struct {
	img image.Image
}

// WriteTo implements the io.WriterTo interface, writing an iTerm-escaped image string.
func (i *ITermImage) WriteTo(w io.Writer) (int64, error) {
	str, err := ITermEncodePNGToString(i.img, "[iTerm Image]")
	if err != nil {
		return 0, err
	}
	n, err := w.Write([]byte(str))
	return int64(n), err
}

// ITermEncodePNGToString encodes an image as a PNG, and returns an iTerm-escaped image string.
func ITermEncodePNGToString(img image.Image, alt string) (str string, err error) {
	b := new(bytes.Buffer)
	err = png.Encode(b, img)
	if err != nil {
		return
	}
	bytes := b.Bytes()
	base64str := base64.StdEncoding.EncodeToString(bytes)
	str = fmt.Sprintf("\033]1337;File=inline=1;size=%d:%s\a%s", len(bytes), base64str, alt)
	return
}

func (i *ITermImage) String() string {
	str, err := ITermEncodePNGToString(i.img, "[iTerm Image]")
	if err != nil {
		return err.Error()
	}
	return str
}
