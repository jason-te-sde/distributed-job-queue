package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"image"
	"image/png"
	"math/rand/v2"
)

// The sizes the worker accepts and produces.
const (
	// MinSize and MaxSize bound the source edge of a job, in pixels.
	MinSize = 64
	MaxSize = 2048
	// ThumbEdge is the edge of a thumbnail, in pixels.
	ThumbEdge = 128
)

// streamSeed is the second word of the generator's seed. It is fixed, so the
// job's seed alone decides the picture.
const streamSeed = 0x7468756d626e6169

// Payload is the input of a job: the picture to draw and its size.
type Payload struct {
	// Seed decides the colours and the rectangles of the picture.
	Seed int64 `json:"seed"`
	// Size is the edge of the source picture in pixels, from 64 to 2048.
	Size int `json:"size"`
}

// Validate checks that the payload can be rendered.
func (p Payload) Validate() error {
	if p.Size < MinSize || p.Size > MaxSize {
		return fmt.Errorf("size %d is outside %d..%d", p.Size, MinSize, MaxSize)
	}
	return nil
}

// Render draws the source picture of a payload: a diagonal gradient between
// two colours of the seed, with a few rectangles of its colours on top. The
// same payload always draws the same pixels, on every machine: the code uses
// integer arithmetic only, because the Go compiler may fuse a float multiply
// and add on some architectures and not on others. Render panics if the size
// is invalid; Validate says whether it is.
func Render(p Payload) *image.RGBA {
	if err := p.Validate(); err != nil {
		panic("thumbnail: Render: " + err.Error())
	}
	rng := rand.New(rand.NewPCG(uint64(p.Seed), streamSeed))
	n := p.Size
	img := image.NewRGBA(image.Rect(0, 0, n, n))

	var from, to [3]int
	for i := range from {
		from[i], to[i] = rng.IntN(256), rng.IntN(256)
	}
	span := 2 * (n - 1) // the largest value of x+y
	for y := range n {
		row := img.Pix[y*img.Stride : y*img.Stride+n*4]
		for x := range n {
			t := (x + y) * 255 / span
			px := row[x*4 : x*4+4]
			for i := range from {
				px[i] = uint8(from[i] + (to[i]-from[i])*t/255)
			}
			px[3] = 255
		}
	}

	for range 6 + rng.IntN(10) {
		w, h := n/8+rng.IntN(n/2), n/8+rng.IntN(n/2)
		x0, y0 := rng.IntN(n-w+1), rng.IntN(n-h+1)
		colour := [4]uint8{uint8(rng.IntN(256)), uint8(rng.IntN(256)), uint8(rng.IntN(256)), 255}
		for y := y0; y < y0+h; y++ {
			row := img.Pix[y*img.Stride : y*img.Stride+n*4]
			for x := x0; x < x0+w; x++ {
				copy(row[x*4:x*4+4], colour[:])
			}
		}
	}
	return img
}

// Downscale shrinks src to edge by edge pixels with a box filter: each pixel
// of the result is the mean of the box of source pixels that it covers, rounded
// half up. The boxes tile the source, so no source pixel is left out when the
// edge does not divide the size. A source smaller than the edge in either
// direction is not enlarged; the result is a copy of it. Downscale panics if
// the edge is below 1.
func Downscale(src *image.RGBA, edge int) *image.RGBA {
	if edge < 1 {
		panic(fmt.Sprintf("thumbnail: Downscale: the edge must be at least 1, got %d", edge))
	}
	b := src.Bounds()
	sw, sh := b.Dx(), b.Dy()
	if edge > sw || edge > sh {
		dst := image.NewRGBA(image.Rect(0, 0, sw, sh))
		for y := range sh {
			from := src.PixOffset(b.Min.X, b.Min.Y+y)
			copy(dst.Pix[y*dst.Stride:y*dst.Stride+sw*4], src.Pix[from:from+sw*4])
		}
		return dst
	}

	dst := image.NewRGBA(image.Rect(0, 0, edge, edge))
	for dy := range edge {
		y0, y1 := dy*sh/edge, (dy+1)*sh/edge
		for dx := range edge {
			x0, x1 := dx*sw/edge, (dx+1)*sw/edge
			var sum [4]uint64
			for y := y0; y < y1; y++ {
				off := src.PixOffset(b.Min.X+x0, b.Min.Y+y)
				for x := x0; x < x1; x++ {
					for i := range sum {
						sum[i] += uint64(src.Pix[off+i])
					}
					off += 4
				}
			}
			count := uint64((x1 - x0) * (y1 - y0))
			out := dst.Pix[dy*dst.Stride+dx*4:]
			for i := range sum {
				out[i] = uint8((sum[i] + count/2) / count)
			}
		}
	}
	return dst
}

// Thumbnail does the whole job: it renders the payload, shrinks the picture to
// ThumbEdge pixels and encodes it as a PNG. sum is the SHA-256 of the PNG as 64
// hex digits. The PNG bytes come from the standard library's encoder, so the
// sum can change with the Go version; the pixels do not.
func Thumbnail(p Payload) (data []byte, sum string, err error) {
	if err := p.Validate(); err != nil {
		return nil, "", fmt.Errorf("thumbnail: %w", err)
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, Downscale(Render(p), ThumbEdge)); err != nil {
		return nil, "", fmt.Errorf("thumbnail: encode: %w", err)
	}
	return buf.Bytes(), sha256Hex(buf.Bytes()), nil
}

// sha256Hex returns the SHA-256 of b as 64 lowercase hex digits.
func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
