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

	"github.com/ttl256/1brcgo/internal/weather"
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
		cancel(fmt.Errorf("received signal: %s", sig))
		go func() {
			sig = <-sigCh
			os.Exit(1)
		}()
	}()

	os.Exit(int(run(ctx)))
}

func run(ctx context.Context) ExitCode {
	var filepath string
	var cpuprofile string
	var debug bool

	flag.StringVar(&filepath, "f", "", "path to a file with input data")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "path to cpuprofile file")
	flag.BoolVar(&debug, "v", false, "enable verbose logging")

	flag.Parse()

	var loggerOptions = new(slog.HandlerOptions)
	loggerOptions.AddSource = false
	loggerOptions.Level = slog.LevelInfo

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

	start := time.Now()

	cities, err := weather.Solution(ctx, f, logger)
	if err != nil {
		logger.LogAttrs(ctx, slog.LevelError, "", slog.Any("error", err))
		return One
	}

	logger.LogAttrs(ctx, slog.LevelInfo, "total number of cities", slog.Int("number", len(cities)))
	logger.LogAttrs(ctx, slog.LevelInfo, "total time", slog.Duration("time", time.Since(start)))
	return Zero
}
