package main

import (
	"context"
	cryptoRand "crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ttl256/1brcgo/internal/measurements"
	"golang.org/x/sync/errgroup"
)

type ExitCode int

const (
	Zero ExitCode = iota
	One
)

func main() {
	ctx, cancel := context.WithCancelCause(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		cancel(fmt.Errorf("received signal: %s. stopping", sig))
		go func() {
			sig = <-sigCh
			os.Exit(int(One))
		}()
	}()

	os.Exit(int(run(ctx)))
}

func run(ctx context.Context) ExitCode {
	const (
		seedLen         = 32            // ChaCha8 seed length
		defaultSize     = 1_000_000_000 // 1brc for a reason
		defaultFilepath = "./measurements.txt"
		chunkSize       = 100_000
	)

	var (
		seedS    string
		filepath string
		size     int
	)

	var loggerOptions = new(slog.HandlerOptions)
	loggerOptions.Level = slog.LevelError
	logger := slog.New(slog.NewTextHandler(os.Stdout, loggerOptions))

	flag.StringVar(&filepath, "f", defaultFilepath, "Path to output file with measurements data")
	flag.IntVar(&size, "c", defaultSize, "Number of rows to generate")
	flag.StringVar(&seedS, "seed", "", fmt.Sprintf("Seed for random number generator."+
		" If len(seed) < %[1]d it's padded with zeroes, if len(seed) > %[1]d it's truncated to %[1]d.", seedLen))
	flag.Parse()

	seed := make([]byte, seedLen)

	if seedS != "" {
		copy(seed, []byte(seedS))
	} else {
		_, err := cryptoRand.Read(seed)
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "", slog.Any("error", err))
			return One
		}
	}

	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logger.LogAttrs(ctx, slog.LevelError, "", slog.Any("error", err))
		return One
	}
	defer f.Close()

	var (
		rowsWritten  atomic.Uint64
		bytesWritten atomic.Uint64
	)

	r := rand.New(rand.NewChaCha8([seedLen]byte(seed))) //nolint: gosec // fine

	sCh := make(chan []string)
	spinnerCh := make(chan string)
	done := make(chan struct{})

	fmt.Println("Starting generation of the dataset to output file " + f.Name()) //nolint: forbidigo // fine

	go spinner(done, spinnerCh, &rowsWritten, size, &bytesWritten)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(sCh)

		return measurements.GenerateDataSet(ctx, r, size, chunkSize, sCh)
	})

	var n int

loop:
	for {
		select {
		case chunk, ok := <-sCh:
			if !ok {
				line := <-spinnerCh
				fmt.Println(line) //nolint: forbidigo // fine
				break loop
			}
			n, err = f.WriteString(strings.Join(chunk, ""))
			if err != nil {
				logger.LogAttrs(ctx, slog.LevelError, "error writing to output file", slog.Any("error", err))
				return One
			}
			rowsWritten.Add(uint64(len(chunk)))
			bytesWritten.Add(uint64(n)) //nolint: gosec // n is always non-negative
		case line := <-spinnerCh:
			fmt.Print(line) //nolint: forbidigo // fine
		}
	}

	err = g.Wait()
	if err != nil {
		return One
	}

	done <- struct{}{}
	close(spinnerCh)

	fmt.Println("All done") //nolint: forbidigo // fine

	return Zero
}

func spinner(
	done <-chan struct{}, outCh chan<- string, rowN *atomic.Uint64, rowsTotal int, bytesN *atomic.Uint64,
) {
	const (
		delay           = 150 * time.Millisecond
		digitsInPercent = 5
		hundred         = 100
	)
	chars := []rune{'\\', '|', '/', '-'}

	getChar := func(chars []rune) func() rune {
		var i int
		return func() rune {
			char := chars[i]
			i = (i + 1) % len(chars)
			return char
		}
	}(chars)

	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			percent := float64(rowN.Load()) / float64(uint64(rowsTotal)) * hundred //nolint: gosec,lll // rowsTotal is always non-negative
			s := fmt.Sprintf(
				"\r%c  %0*.1f%% %s", getChar(), digitsInPercent, percent, humanize.Bytes(bytesN.Load()),
			)
			select {
			case <-done:
				return
			case outCh <- s:
			}
		}
	}
}
