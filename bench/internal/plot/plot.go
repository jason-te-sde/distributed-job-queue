// Package plot draws the empirical cumulative distribution of a wait as an SVG.
//
// The document is written with text/template from the standard library, with
// every coordinate computed in Go, so the output depends on nothing but the
// data. The x axis is logarithmic, in milliseconds, because the waits that the
// experiments compare differ by orders of magnitude.
package plot

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"text/template"
	"time"
)

// Point is one point of a distribution: Fraction of the samples waited at most
// Wait.
type Point struct {
	Wait     time.Duration
	Fraction float64
}

// Series is one curve and the label of its legend entry.
type Series struct {
	Label  string
	Points []Point
}

const (
	width, height             = 800.0, 520.0
	left, right               = 72.0, 48.0
	top, bottom               = 24.0, 128.0
	minWait                   = time.Microsecond // the floor of the log axis: a zero wait has no place on it
	fractionTicks             = 4
	legendWidth, legendGap    = 150.0, 14.0
	swatch                    = 26.0
	titleOffset, titleSpacing = 92.0, 16.0
	maxTitleLines             = 3
)

// colors and dashes tell the series apart by more than hue.
var (
	colors = []string{"#1f77b4", "#d95f02", "#2ca25f", "#756bb1"}
	dashes = []string{"", "7 4", "2 3", "10 3 2 3"}
)

type tick struct {
	Pos   string
	Label string
}

type seriesView struct {
	Label string
	D     string
	Color string
	Dash  string
	// LegendX, LegendX2 and LegendY place the swatch, a short sample of the
	// line; the label sits right of it.
	LegendX, LegendX2, LegendY string
}

// titleLine is one line of the title.
type titleLine struct {
	X, Y, Text string
}

type view struct {
	Width, Height            string
	Left, Right, Top, Bottom string // the edges of the plot area
	XLabel, YLabel           string
	TitleLines               []titleLine
	XLabelX, XLabelY         string
	YLabelX, YLabelY         string
	XTicks, YTicks           []tick
	Series                   []seriesView
}

// Render writes an SVG of the series to w, with the title below the x axis.
// The title is where a plot names the parameters of the run that produced it;
// each line of it, up to three, is a line of text.
// It is an error to draw no series, or a series without points.
func Render(w io.Writer, series []Series, title string) error {
	if len(series) == 0 {
		return errors.New("plot: there is no series to draw")
	}
	if len(series) > len(colors) {
		return fmt.Errorf("plot: %d series, want at most %d", len(series), len(colors))
	}
	if strings.Count(title, "\n") >= maxTitleLines {
		return fmt.Errorf("plot: the title has more than %d lines", maxTitleLines)
	}
	for _, s := range series {
		if len(s.Points) == 0 {
			return fmt.Errorf("plot: the series %q has no points", s.Label)
		}
	}

	loDecade, hiDecade := decades(series)
	plotW, plotH := width-left-right, height-top-bottom
	x := func(wait time.Duration) float64 {
		d := math.Log10(float64(max(wait, minWait)) / float64(time.Millisecond))
		return left + (d-loDecade)/(hiDecade-loDecade)*plotW
	}
	y := func(fraction float64) float64 { return top + (1-min(max(fraction, 0), 1))*plotH }

	v := view{
		Width: num(width), Height: num(height),
		Left: num(left), Right: num(width - right), Top: num(top), Bottom: num(top + plotH),
		XLabel: "wait before the job started (ms, log scale)", YLabel: "fraction of jobs started",
		XLabelX: num(left + plotW/2), XLabelY: num(top + plotH + 38),
		YLabelX: "20", YLabelY: num(top + plotH/2),
	}
	for i, line := range strings.Split(title, "\n") {
		v.TitleLines = append(v.TitleLines, titleLine{X: num(left), Y: num(top + plotH + titleOffset + float64(i)*titleSpacing), Text: line})
	}
	for d := loDecade; d <= hiDecade; d++ {
		v.XTicks = append(v.XTicks, tick{
			Pos:   num(left + (d-loDecade)/(hiDecade-loDecade)*plotW),
			Label: strconv.FormatFloat(math.Pow(10, d), 'f', -1, 64),
		})
	}
	for i := 0; i <= fractionTicks; i++ {
		f := float64(i) / fractionTicks
		v.YTicks = append(v.YTicks, tick{Pos: num(y(f)), Label: strconv.FormatFloat(f, 'f', -1, 64)})
	}
	for i, s := range series {
		sv := seriesView{
			Label: s.Label, Color: colors[i], Dash: dashes[i],
			LegendX: num(left + float64(i)*(legendWidth+legendGap)), LegendX2: num(left + float64(i)*(legendWidth+legendGap) + swatch),
			LegendY: num(top + plotH + 62),
		}
		sv.D = stepPath(s.Points, x, y, top+plotH)
		v.Series = append(v.Series, sv)
	}

	var buf bytes.Buffer
	if err := svgTemplate.Execute(&buf, v); err != nil {
		return fmt.Errorf("plot: render the SVG: %w", err)
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// stepPath is the staircase through the points: the distribution is flat until
// a sample and jumps by its share at the sample's wait. The path uses absolute
// M, H and V commands only, with space-separated numbers.
func stepPath(pts []Point, x func(time.Duration) float64, y func(float64) float64, base float64) string {
	var b bytes.Buffer
	for i, p := range pts {
		if i == 0 {
			fmt.Fprintf(&b, "M %s %s V %s", num(x(p.Wait)), num(base), num(y(p.Fraction)))
			continue
		}
		fmt.Fprintf(&b, " H %s V %s", num(x(p.Wait)), num(y(p.Fraction)))
	}
	return b.String()
}

// decades returns the decades of milliseconds, as whole powers of ten, that
// the x axis spans: the one below the smallest wait to the one above the largest,
// and at least one decade wide.
func decades(series []Series) (lo, hi float64) {
	smallest, largest := time.Duration(math.MaxInt64), time.Duration(0)
	for _, s := range series {
		for _, p := range s.Points {
			smallest, largest = min(smallest, max(p.Wait, minWait)), max(largest, p.Wait)
		}
	}
	lo = math.Floor(math.Log10(float64(smallest) / float64(time.Millisecond)))
	hi = math.Ceil(math.Log10(float64(max(largest, minWait)) / float64(time.Millisecond)))
	if hi <= lo {
		hi = lo + 1
	}
	return lo, hi
}

// num formats a coordinate with two decimals.
func num(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }

// escape makes s safe as XML character data and attribute value alike.
func escape(s string) (string, error) {
	var b bytes.Buffer
	if err := xml.EscapeText(&b, []byte(s)); err != nil {
		return "", err
	}
	return b.String(), nil
}

var svgTemplate = template.Must(template.New("cdf.svg").Funcs(template.FuncMap{"esc": escape}).Parse(
	`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {{.Width}} {{.Height}}" width="{{.Width}}" height="{{.Height}}" font-family="sans-serif" font-size="12">
<rect x="0" y="0" width="{{.Width}}" height="{{.Height}}" fill="#ffffff"/>
{{range .XTicks}}<line x1="{{.Pos}}" y1="{{$.Top}}" x2="{{.Pos}}" y2="{{$.Bottom}}" stroke="#e3e3e3"/>
<text x="{{.Pos}}" y="{{$.Bottom}}" dy="16" text-anchor="middle" fill="#333333">{{esc .Label}}</text>
{{end}}{{range .YTicks}}<line x1="{{$.Left}}" y1="{{.Pos}}" x2="{{$.Right}}" y2="{{.Pos}}" stroke="#e3e3e3"/>
<text x="{{$.Left}}" y="{{.Pos}}" dx="-8" dy="4" text-anchor="end" fill="#333333">{{esc .Label}}</text>
{{end}}<line x1="{{.Left}}" y1="{{.Bottom}}" x2="{{.Right}}" y2="{{.Bottom}}" stroke="#333333"/>
<line x1="{{.Left}}" y1="{{.Top}}" x2="{{.Left}}" y2="{{.Bottom}}" stroke="#333333"/>
<text x="{{.XLabelX}}" y="{{.XLabelY}}" text-anchor="middle" fill="#333333">{{esc .XLabel}}</text>
<text x="{{.YLabelX}}" y="{{.YLabelY}}" text-anchor="middle" fill="#333333" transform="rotate(-90 {{.YLabelX}} {{.YLabelY}})">{{esc .YLabel}}</text>
{{range .Series}}<path d="{{.D}}" fill="none" stroke="{{.Color}}" stroke-width="2"{{if .Dash}} stroke-dasharray="{{.Dash}}"{{end}}/>
{{end}}{{range .Series}}<line x1="{{.LegendX}}" y1="{{.LegendY}}" x2="{{.LegendX2}}" y2="{{.LegendY}}" stroke="{{.Color}}" stroke-width="2"{{if .Dash}} stroke-dasharray="{{.Dash}}"{{end}}/>
<text x="{{.LegendX2}}" y="{{.LegendY}}" dx="8" dy="4" fill="#222222">{{esc .Label}}</text>
{{end}}{{range .TitleLines}}<text class="title" x="{{.X}}" y="{{.Y}}" fill="#555555">{{esc .Text}}</text>
{{end}}</svg>
`))
