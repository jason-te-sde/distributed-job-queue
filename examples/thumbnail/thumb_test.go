package main

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
)

func TestThumbnailDeterministic(t *testing.T) {
	_, a1, err := Thumbnail(Payload{Seed: 1, Size: 256})
	if err != nil {
		t.Fatal(err)
	}
	_, a2, err := Thumbnail(Payload{Seed: 1, Size: 256})
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := Thumbnail(Payload{Seed: 2, Size: 256})
	if err != nil {
		t.Fatal(err)
	}
	if a1 != a2 {
		t.Errorf("seed 1 gave two sums: %s and %s", a1, a2)
	}
	if a1 == b {
		t.Errorf("seeds 1 and 2 gave the same sum %s", a1)
	}
	if len(a1) != 64 {
		t.Errorf("sum %q is not 64 hex characters", a1)
	}
}

func TestThumbnailEncodesA128PNGAndSumsTheBytes(t *testing.T) {
	data, sum, err := Thumbnail(Payload{Seed: 7, Size: 512})
	if err != nil {
		t.Fatal(err)
	}
	img, err := png.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("the result is not a PNG: %v", err)
	}
	if got := img.Bounds().Size(); got != (image.Point{X: 128, Y: 128}) {
		t.Errorf("thumbnail is %v, want 128x128", got)
	}
	if want := sha256Hex(data); sum != want {
		t.Errorf("sum = %s, want the SHA-256 of the PNG bytes, %s", sum, want)
	}
}

func TestDownscaleDimensions(t *testing.T) {
	for _, tc := range []struct{ size, want int }{{1024, 128}, {64, 64}, {128, 128}, {129, 128}} {
		src := Render(Payload{Seed: 3, Size: tc.size})
		got := Downscale(src, 128).Bounds().Size()
		if got != (image.Point{X: tc.want, Y: tc.want}) {
			t.Errorf("Downscale of %d: got %v, want %dx%d", tc.size, got, tc.want, tc.want)
		}
	}
}

func TestRenderRejectsBadSize(t *testing.T) {
	for _, size := range []int{-1, 0, 10, 63, 2049, 4096} {
		if _, _, err := Thumbnail(Payload{Seed: 1, Size: size}); err == nil {
			t.Errorf("Thumbnail accepted size %d", size)
		}
	}
	if _, _, err := Thumbnail(Payload{Seed: 1, Size: 64}); err != nil {
		t.Errorf("Thumbnail refused the smallest size, 64: %v", err)
	}
	if err := (Payload{Seed: 1, Size: 2048}).Validate(); err != nil {
		t.Errorf("Validate refused the largest size, 2048: %v", err)
	}
}

func TestRenderPanicsOnBadSize(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Render did not panic on size 10")
		}
	}()
	Render(Payload{Seed: 1, Size: 10})
}

func TestDownscaleAveragesEachBox(t *testing.T) {
	// A 4x4 image whose red channel counts the pixels, so every 2x2 box has a
	// known mean. The mean rounds half up: (0+1+4+5)/4 = 2.5 -> 3.
	src := image.NewRGBA(image.Rect(0, 0, 4, 4))
	for y := range 4 {
		for x := range 4 {
			src.SetRGBA(x, y, color.RGBA{R: uint8(y*4 + x), G: 200, B: 0, A: 255})
		}
	}
	got := Downscale(src, 2)
	want := [2][2]uint8{{3, 5}, {11, 13}}
	for y := range 2 {
		for x := range 2 {
			c := got.RGBAAt(x, y)
			if c.R != want[y][x] || c.G != 200 || c.B != 0 || c.A != 255 {
				t.Errorf("pixel (%d,%d) = %v, want R=%d G=200 B=0 A=255", x, y, c, want[y][x])
			}
		}
	}
}

func TestDownscaleUnevenBoxesCoverEverySourcePixel(t *testing.T) {
	// 5 -> 2: the boxes are columns [0,2) and [2,5), so the last pixel counts.
	src := image.NewRGBA(image.Rect(0, 0, 5, 5))
	for y := range 5 {
		for x := range 5 {
			v := uint8(0)
			if x == 4 {
				v = 90
			}
			src.SetRGBA(x, y, color.RGBA{R: v, A: 255})
		}
	}
	got := Downscale(src, 2)
	if r := got.RGBAAt(0, 0).R; r != 0 {
		t.Errorf("left box = %d, want 0", r)
	}
	if r := got.RGBAAt(1, 0).R; r != 30 { // 90 spread over a 3-pixel row
		t.Errorf("right box = %d, want 30", r)
	}
}

func TestDownscaleCopiesWhenEdgeIsLarger(t *testing.T) {
	src := Render(Payload{Seed: 5, Size: 64})
	got := Downscale(src, 128)
	if !bytes.Equal(got.Pix, src.Pix) {
		t.Error("the copy differs from the source")
	}
	got.Pix[0] ^= 0xff
	if got.Pix[0] == src.Pix[0] {
		t.Error("the result shares its pixels with the source")
	}
}

func TestRenderIsOpaque(t *testing.T) {
	img := Render(Payload{Seed: 11, Size: 64})
	for i := 3; i < len(img.Pix); i += 4 {
		if img.Pix[i] != 255 {
			t.Fatalf("pixel %d has alpha %d, want 255", i/4, img.Pix[i])
		}
	}
}

// TestPixelsDoNotDependOnTheMachine pins the pixels of two renders. The code
// uses integer arithmetic only, because the Go compiler may fuse a float
// multiply and add on arm64 but not on amd64, which would make the same seed
// render differently on two machines. The values were taken from the
// implementation, so this test guards against change, not against error; CI
// runs it on amd64 and development on arm64.
func TestPixelsDoNotDependOnTheMachine(t *testing.T) {
	for _, tc := range []struct {
		name string
		img  *image.RGBA
		want string
	}{
		{"render seed 1 size 256", Render(Payload{Seed: 1, Size: 256}), "4dc43d21df65636e039037097ad160c212e1a31b0d0d83362839bd076e6c0eda"},
		{"downscale of render seed 2 size 1024", Downscale(Render(Payload{Seed: 2, Size: 1024}), 128), "80a5647911468a450e5384809ecee3c284c2f732dad75ffb62a0975199547a48"},
	} {
		if got := sha256Hex(tc.img.Pix); got != tc.want {
			t.Errorf("%s: pixel hash = %s, want %s", tc.name, got, tc.want)
		}
	}
}
