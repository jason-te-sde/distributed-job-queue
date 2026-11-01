package plot

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

// cdf is a staircase of n samples from lo to hi, spaced evenly in log scale.
func cdf(lo, hi time.Duration, n int) []Point {
	pts := make([]Point, n)
	for i := range n {
		f := float64(i+1) / float64(n)
		w := time.Duration(float64(lo) * math.Pow(float64(hi)/float64(lo), float64(i)/float64(n-1)))
		pts[i] = Point{Wait: w, Fraction: f}
	}
	return pts
}

type element struct {
	name  string
	attrs map[string]string
	text  string // the character data directly inside the element
}

// parse reads every element of the document, and fails the test if the
// document is not well-formed XML.
func parse(t *testing.T, svg []byte) (elements []element, text string) {
	t.Helper()
	dec := xml.NewDecoder(bytes.NewReader(svg))
	var chars strings.Builder
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return elements, chars.String()
		}
		if err != nil {
			t.Fatalf("the SVG is not well-formed XML: %v\n%s", err, svg)
		}
		switch tok := tok.(type) {
		case xml.StartElement:
			e := element{name: tok.Name.Local, attrs: map[string]string{}}
			for _, a := range tok.Attr {
				e.attrs[a.Name.Local] = a.Value
			}
			elements = append(elements, e)
		case xml.CharData:
			chars.Write(tok)
			chars.WriteByte('\n')
			if len(elements) > 0 {
				elements[len(elements)-1].text += string(tok)
			}
		}
	}
}

func render(t *testing.T, series []Series, title string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Render(&buf, series, title); err != nil {
		t.Fatalf("Render: %v", err)
	}
	return buf.Bytes()
}

func TestPlotSVGIsWellFormed(t *testing.T) {
	series := []Series{
		{Label: "FIFO", Points: cdf(2*time.Millisecond, 20*time.Second, 120)},
		{Label: "DRR", Points: cdf(500*time.Microsecond, 80*time.Millisecond, 120)},
	}
	svg := render(t, series, "tenant A floods 100000 jobs, tenant B sends 10/s; 1 broker, 2 workers")
	elements, text := parse(t, svg)

	if elements[0].name != "svg" {
		t.Fatalf("the root element is %q, want svg", elements[0].name)
	}
	paths := 0
	for _, e := range elements {
		if e.name == "path" {
			paths++
			if !strings.HasPrefix(e.attrs["d"], "M") {
				t.Errorf("a path has d=%q, want it to start with a move command", e.attrs["d"])
			}
		}
	}
	if paths != 2 {
		t.Errorf("the SVG has %d <path> elements, want exactly 2, one per series", paths)
	}
	for _, label := range []string{"FIFO", "DRR"} {
		if !strings.Contains(text, label) {
			t.Errorf("the legend has no %q: text nodes are %q", label, text)
		}
	}
	if !strings.Contains(text, "tenant A floods 100000 jobs") {
		t.Errorf("the axis title does not name the run parameters: %q", text)
	}
	if !strings.Contains(text, "ms") {
		t.Errorf("the x axis is not labelled in milliseconds: %q", text)
	}
}

func TestPlotXAxisIsLogScale(t *testing.T) {
	// One point per decade of milliseconds: on a log axis they are evenly
	// spaced, so the x steps between the path's vertices are equal.
	pts := []Point{
		{Wait: time.Millisecond, Fraction: 0.25},
		{Wait: 10 * time.Millisecond, Fraction: 0.5},
		{Wait: 100 * time.Millisecond, Fraction: 0.75},
		{Wait: time.Second, Fraction: 1},
	}
	elements, _ := parse(t, render(t, []Series{{Label: "x", Points: pts}}, "t"))
	var d string
	for _, e := range elements {
		if e.name == "path" {
			d = e.attrs["d"]
		}
	}
	xs := vertexXs(t, d)
	if len(xs) < 4 {
		t.Fatalf("path %q has %d x values, want at least 4", d, len(xs))
	}
	// A step path repeats each x for the vertical riser, so take the distinct ones.
	var distinct []float64
	for _, x := range xs {
		if len(distinct) == 0 || x != distinct[len(distinct)-1] {
			distinct = append(distinct, x)
		}
	}
	if len(distinct) != 4 {
		t.Fatalf("path %q has %d distinct x values, want 4", d, len(distinct))
	}
	steps := []float64{distinct[1] - distinct[0], distinct[2] - distinct[1], distinct[3] - distinct[2]}
	for _, s := range steps[1:] {
		if diff := s - steps[0]; diff > 0.5 || diff < -0.5 {
			t.Errorf("x steps between decades are %v, want equal on a log axis", steps)
		}
	}
}

func TestPlotEscapesLabelsAndTitle(t *testing.T) {
	series := []Series{{Label: `A & B <"x">`, Points: cdf(time.Millisecond, time.Second, 10)}}
	_, text := parse(t, render(t, series, `a < b && "c"`))
	if !strings.Contains(text, `A & B <"x">`) || !strings.Contains(text, `a < b && "c"`) {
		t.Errorf("the label or title did not survive escaping: %q", text)
	}
}

func TestPlotHandlesOnePointAndZeroWait(t *testing.T) {
	series := []Series{{Label: "one", Points: []Point{{Wait: 0, Fraction: 1}}}}
	elements, _ := parse(t, render(t, series, "t"))
	n := 0
	for _, e := range elements {
		if e.name == "path" {
			n++
			if strings.Contains(e.attrs["d"], "NaN") || strings.Contains(e.attrs["d"], "Inf") {
				t.Errorf("path %q holds a non-finite number: a zero wait has no place on a log axis", e.attrs["d"])
			}
		}
	}
	if n != 1 {
		t.Errorf("%d paths, want 1", n)
	}
}

func TestPlotRefusesNothingToDraw(t *testing.T) {
	var buf bytes.Buffer
	if err := Render(&buf, nil, "t"); err == nil {
		t.Error("Render with no series returned no error")
	}
	if err := Render(&buf, []Series{{Label: "empty"}}, "t"); err == nil {
		t.Error("Render with a series of no points returned no error")
	}
}

// vertexXs returns the x coordinate of every vertex of a path made of M, L, H
// and V commands with absolute coordinates, in the order they appear.
func vertexXs(t *testing.T, d string) []float64 {
	t.Helper()
	var xs []float64
	fields := strings.Fields(d)
	for i := 0; i < len(fields); i++ {
		switch fields[i] {
		case "M", "L":
			if i+2 >= len(fields) {
				t.Fatalf("path %q ends inside a command", d)
			}
			xs = append(xs, parseFloat(t, fields[i+1]))
			i += 2
		case "H":
			xs = append(xs, parseFloat(t, fields[i+1]))
			i++
		case "V":
			i++
		default:
			t.Fatalf("path %q holds %q, want M, L, H and V commands with space-separated numbers", d, fields[i])
		}
	}
	return xs
}

func parseFloat(t *testing.T, s string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("%q is not a number: %v", s, err)
	}
	return v
}

func TestPlotTitleMayHaveSeveralLines(t *testing.T) {
	series := []Series{{Label: "x", Points: cdf(time.Millisecond, time.Second, 10)}}
	elements, text := parse(t, render(t, series, "first line of parameters\nsecond line of parameters\nthird"))
	for _, want := range []string{"first line of parameters", "second line of parameters", "third"} {
		if !strings.Contains(text, want) {
			t.Errorf("the title line %q is missing from the text nodes %q", want, text)
		}
	}
	// Each line is a text node of its own, below the one before.
	var ys []float64
	for _, e := range elements {
		if e.name == "text" && e.attrs["class"] == "title" {
			ys = append(ys, parseFloat(t, e.attrs["y"]))
		}
	}
	if len(ys) != 3 {
		t.Fatalf("title lines are at y = %v, want three", ys)
	}
	if !(ys[0] < ys[1] && ys[1] < ys[2]) {
		t.Errorf("title lines are at y = %v, want increasing order", ys)
	}
	if limit := parseFloat(t, elements[0].attrs["height"]); ys[2] > limit {
		t.Errorf("the last title line is at y = %v, outside the %v high drawing", ys[2], limit)
	}
}

func TestPlotKeepsEverythingInsideTheDrawing(t *testing.T) {
	// Waits from microseconds to the hour that the histogram tops out at put the
	// axis labels at their widest.
	series := []Series{
		{Label: "FIFO", Points: cdf(50*time.Microsecond, time.Hour, 80)},
		{Label: "DRR", Points: cdf(time.Millisecond, time.Second, 80)},
	}
	elements, _ := parse(t, render(t, series, "a title\nof two lines"))
	width, height := parseFloat(t, elements[0].attrs["width"]), parseFloat(t, elements[0].attrs["height"])
	const charWidth = 7.0 // generous for a 12px sans-serif face
	for _, e := range elements[1:] {
		for _, attr := range []string{"x", "x1", "x2"} {
			if v, ok := e.attrs[attr]; ok && e.name != "rect" {
				if v := parseFloat(t, v); v < 0 || v > width {
					t.Errorf("<%s %s=%v> is outside the drawing of width %v", e.name, attr, v, width)
				}
			}
		}
		for _, attr := range []string{"y", "y1", "y2"} {
			if v, ok := e.attrs[attr]; ok && e.name != "rect" {
				if v := parseFloat(t, v); v < 0 || v > height {
					t.Errorf("<%s %s=%v> is outside the drawing of height %v", e.name, attr, v, height)
				}
			}
		}
		if e.name == "text" && e.attrs["text-anchor"] == "middle" && e.attrs["transform"] == "" {
			label := strings.TrimSpace(e.text)
			x, half := parseFloat(t, e.attrs["x"]), float64(len(label))*charWidth/2
			if x-half < 0 || x+half > width {
				t.Errorf("the label %q centred at x=%v is wider than the room the drawing leaves it (width %v)", label, x, width)
			}
		}
	}
}
