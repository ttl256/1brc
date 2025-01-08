package weather

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
		panic(fmt.Sprintf("expected to have ';' as a separator in string %q", s))
	}

	temperature, err := strconv.ParseFloat(temperatureRaw, 64)
	if err != nil {
		return Measurement{}, err
	}

	return Measurement{Name: city, Temperature: temperature}, nil
}

func BuildMap(ctx context.Context, f *os.File, logger *slog.Logger) (CityMap, error) {
	const (
		mapSize = 10000
		bufSize = 32
	)

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

func Solution(ctx context.Context, f *os.File, logger *slog.Logger) ([]Result, error) {
	const roundingRatio = 10.0 // round to one decimal place

	m, err := BuildMap(ctx, f, logger)
	if err != nil {
		return nil, err
	}

	logger.LogAttrs(ctx, slog.LevelDebug, "started building final slice")

	rs := make([]Result, 0, len(m))

	for k, v := range m {
		resultCity := Result{
			Name: k,
			Min:  v.Min,
			Avg:  math.Round(v.Total/float64(v.Count)*roundingRatio) / roundingRatio,
			Max:  v.Max,
		}
		rs = append(rs, resultCity)
	}

	slices.SortStableFunc(rs, func(a Result, b Result) int {
		return strings.Compare(a.Name, b.Name)
	})

	logger.LogAttrs(ctx, slog.LevelDebug, "finished building final slice")

	return rs, nil
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
