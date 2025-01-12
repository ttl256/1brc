package onebrc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

const mapSize = 10000

type Measurement struct {
	Name        string
	Temperature float64
}

type City struct {
	Count int
	Total float64
	Min   float64
	Max   float64
}

type Result struct {
	Name string
	Min  float64
	Avg  float64
	Max  float64
}

type CityMap map[string]*City

func ParseRow(s string) (Measurement, error) {
	city, temperatureRaw, found := strings.Cut(s, ";")
	if !found {
		return Measurement{}, fmt.Errorf("expected to have ';' as a separator in string %q", s)
	}

	temperature, err := strconv.ParseFloat(temperatureRaw, 64)
	if err != nil {
		return Measurement{}, err
	}

	return Measurement{Name: city, Temperature: temperature}, nil
}

func BuildMap(ctx context.Context, f *os.File, logger *slog.Logger) (CityMap, error) {
	const bufSize = 32

	numWorkers := runtime.NumCPU() - 1
	logger.LogAttrs(ctx, slog.LevelDebug, "number of workers", slog.Int("number", numWorkers))

	cityMap := make(CityMap, mapSize)
	partialMapsCh := make(chan CityMap, numWorkers)

	chunkCh := make(chan []byte, bufSize)
	const chunkSize = 32 * 1024 * 1024

	g, ctx := errgroup.WithContext(ctx)

	// Read chunks of bytes from a file.
	g.Go(func() error {
		defer close(chunkCh)

		return ChunkifyFile(ctx, f, chunkSize, []byte{'\n'}, chunkCh, logger)
	})

	wg := &sync.WaitGroup{}

	// Workers process chunks of bytes into partial maps of cities temperature.
	for range numWorkers {
		wg.Add(1)

		g.Go(func() error {
			defer wg.Done()

			return ProcessChunks(ctx, chunkCh, partialMapsCh, logger)
		})
	}

	go func() {
		wg.Wait()
		close(partialMapsCh)
	}()

	for m := range partialMapsCh {
		logger.LogAttrs(ctx, slog.LevelDebug, "started processing partial maps")
		for city, v := range m {
			if measurementGlobal, ok := cityMap[city]; ok {
				measurementGlobal.Total += v.Total
				measurementGlobal.Count += v.Count
				if v.Min < measurementGlobal.Min {
					measurementGlobal.Min = v.Min
				}
				if v.Max > measurementGlobal.Max {
					measurementGlobal.Max = v.Max
				}
			} else {
				cityMap[city] = v
			}
		}
		logger.LogAttrs(ctx, slog.LevelDebug, "finished processing partial maps")
	}

	return cityMap, g.Wait()
}

func Solution(ctx context.Context, f *os.File, logger *slog.Logger) (*CityResults, error) {
	m, err := BuildMap(ctx, f, logger)
	if err != nil {
		return nil, err
	}

	logger.LogAttrs(ctx, slog.LevelDebug, "started building final slice")

	s := CityResultsFromMap(m)

	logger.LogAttrs(ctx, slog.LevelDebug, "finished building final slice")

	return s, nil
}

func ChunkifyFile(
	ctx context.Context, f *os.File, chunkSize int, sep []byte, out chan<- []byte, logger *slog.Logger,
) error {
	buf := make([]byte, chunkSize)
	leftover := make([]byte, 0)

	var chunkSeq int

	for {
		chunkSeq++
		logger.LogAttrs(ctx, slog.LevelDebug, "started reading a chunk", slog.Int("seq", chunkSeq))
		bytesRead, err := f.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		chunkToSend := make([]byte, len(leftover)+bytesRead)
		copy(chunkToSend, leftover)
		copy(chunkToSend[len(leftover):], buf[:bytesRead])

		newLineIndex := bytes.LastIndex(chunkToSend, sep)
		if newLineIndex == -1 {
			newLineIndex = len(chunkToSend) - 1
		}

		leftover = make([]byte, len(chunkToSend[newLineIndex+1:]))
		copy(leftover, chunkToSend[newLineIndex+1:])

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case out <- chunkToSend[:newLineIndex+1]:
			logger.LogAttrs(ctx, slog.LevelDebug, "sent a chunk", slog.Int("seq", chunkSeq))
		}
	}

	if len(leftover) > 0 {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case out <- leftover:
			logger.LogAttrs(ctx, slog.LevelDebug, "sent leftover chunk")
		}
	}

	return nil
}

func ProcessChunks( //nolint: gocognit // expected
	ctx context.Context, chunks <-chan []byte, maps chan<- CityMap, logger *slog.Logger,
) error {
	partialMap := make(CityMap)

loop:
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case chunk, chanOk := <-chunks:
			logger.LogAttrs(ctx, slog.LevelDebug, "started processing a chunk")
			if !chanOk {
				break loop
			}
			r := bytes.NewReader(chunk)
			scanner := bufio.NewScanner(r)

			for scanner.Scan() {
				line := scanner.Text()
				measurement, err := ParseRow(line)
				if err != nil {
					return err
				}

				if city, ok := partialMap[measurement.Name]; ok {
					city.Count++
					city.Total += measurement.Temperature
					if measurement.Temperature < city.Min {
						city.Min = measurement.Temperature
					}
					if measurement.Temperature > city.Max {
						city.Max = measurement.Temperature
					}
				} else {
					partialMap[measurement.Name] = &City{
						Count: 1,
						Total: measurement.Temperature,
						Min:   measurement.Temperature,
						Max:   measurement.Temperature,
					}
				}
			}

			if err := scanner.Err(); err != nil {
				return err
			}
			logger.LogAttrs(ctx, slog.LevelDebug, "finished processing a chunk")
		}
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case maps <- partialMap:
		logger.LogAttrs(ctx, slog.LevelDebug, "sent a partial map")
	}

	return nil
}

func SolutionBaseline(r io.Reader) (*CityResults, error) {
	cityMap := make(CityMap, mapSize)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		measurement, err := ParseRow(scanner.Text())
		if err != nil {
			return nil, err
		}

		if city, ok := cityMap[measurement.Name]; ok {
			city.Count++
			city.Total += measurement.Temperature
			if measurement.Temperature < city.Min {
				city.Min = measurement.Temperature
			}
			if measurement.Temperature > city.Max {
				city.Max = measurement.Temperature
			}
		} else {
			cityMap[measurement.Name] = &City{
				Count: 1,
				Total: measurement.Temperature,
				Min:   measurement.Temperature,
				Max:   measurement.Temperature,
			}
		}
	}

	return CityResultsFromMap(cityMap), nil
}

type CityResults struct {
	Cities []Result
}

func NewCityResults(s []Result) *CityResults {
	return &CityResults{Cities: s}
}

// {city1=min/avg/max, city2=min/avg/max}.
func (c *CityResults) String() string {
	buf := &strings.Builder{}

	buf.WriteByte('{')
	for i, r := range c.Cities {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(r.Name)
		buf.WriteByte('=')
		buf.WriteString(strconv.FormatFloat(r.Min, 'f', 1, 64))
		buf.WriteByte('/')
		buf.WriteString(strconv.FormatFloat(r.Avg, 'f', 1, 64))
		buf.WriteByte('/')
		buf.WriteString(strconv.FormatFloat(r.Max, 'f', 1, 64))
	}
	buf.WriteByte('}')

	return buf.String()
}

func (c *CityResults) Len() int {
	return len(c.Cities)
}

func CityResultsFromMap(m CityMap) *CityResults {
	s := make([]Result, 0, len(m))

	for k, v := range m {
		s = append(s, Result{
			Name: k,
			Min:  v.Min,
			Avg:  CeilToOneDecimalPlace(v.Total / float64(v.Count)),
			Max:  v.Max,
		})
	}

	slices.SortStableFunc(s, func(a Result, b Result) int {
		return strings.Compare(a.Name, b.Name)
	})

	return NewCityResults(s)
}

func CeilToOneDecimalPlace(x float64) float64 {
	const roundingRatio = 10.0 // round to one decimal place

	return math.Ceil(x*roundingRatio) / roundingRatio
}
