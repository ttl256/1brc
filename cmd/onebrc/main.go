package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/ttl256/1brcgo/internal/onebrc"
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
	var filepath string
	var cpuprofile string
	var baseline bool
	var printB bool
	var debug bool

	flag.StringVar(&filepath, "f", "", "path to a file with input data")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "path to cpuprofile file")
	flag.BoolVar(&baseline, "baseline", false, "run baseline implementation")
	flag.BoolVar(&printB, "print", false, "print resulting string")
	flag.BoolVar(&debug, "v", false, "enable verbose logging")

	flag.Parse()

	var loggerOptions = new(slog.HandlerOptions)
	loggerOptions.AddSource = false
	loggerOptions.Level = slog.LevelError

	if debug {
		loggerOptions.AddSource = true
		loggerOptions.Level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, loggerOptions))

	if filepath == "" {
		logger.LogAttrs(ctx, slog.LevelError, "provide a path to a file with input data")
		return One
	}

	if cpuprofile != "" {
		f, err := os.Create("./profiles/" + cpuprofile)
		if err != nil {
			logger.LogAttrs(
				ctx, slog.LevelError, "error creating file for cpuprofile", slog.String("file", cpuprofile), slog.Any("error", err),
			)
			return One
		}
		defer f.Close()

		if errProf := pprof.StartCPUProfile(f); errProf != nil {
			logger.LogAttrs(ctx, slog.LevelError, "error starting cpuprofile", slog.Any("error", errProf))
			return One
		}
		defer pprof.StopCPUProfile()
	}

	f, err := os.Open(filepath)
	if err != nil {
		logger.LogAttrs(ctx, slog.LevelError, "", slog.Any("error", err))
		return One
	}
	defer f.Close()

	logger.LogAttrs(ctx, slog.LevelDebug, "starting")
	defer logger.LogAttrs(ctx, slog.LevelDebug, "stopping")

	start := time.Now()

	var cities *onebrc.CityResults

	if baseline {
		cities, err = onebrc.SolutionBaseline(f)
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "", slog.Any("error", err))
			return One
		}
	} else {
		cities, err = onebrc.Solution(ctx, f, logger)
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "", slog.Any("error", err))
			return One
		}
	}

	if printB {
		fmt.Println(cities.String()) //nolint: forbidigo // fine
	}

	logger.LogAttrs(ctx, slog.LevelInfo, "total number of cities", slog.Int("number", cities.Len()))
	logger.LogAttrs(ctx, slog.LevelInfo, "total time", slog.Duration("time", time.Since(start)))
	return Zero
}
