package weather_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ttl256/1brcgo/internal/weather"
)

var defaultLogger = slog.New( //nolint: gochecknoglobals // let me be
	slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}), //nolint: exhaustruct // let me be
)

func TestParseRow(t *testing.T) {
	cases := []struct {
		input string
		want  weather.Measurement
	}{
		{input: "Sharya;92.4", want: weather.Measurement{Name: "Sharya", Temperature: 92.4}},
		{input: "Đà Nẵng;17.0", want: weather.Measurement{Name: "Đà Nẵng", Temperature: 17.0}},
		{input: "Saint-Saulve;-40.3", want: weather.Measurement{Name: "Saint-Saulve", Temperature: -40.3}},
	}

	for _, tt := range cases {
		t.Run(tt.input, func(t *testing.T) {
			got, err := weather.ParseRow(tt.input)
			if err != nil {
				t.Errorf("did not expect an error, got %v", err)
			}

			if tt.want != got {
				t.Error(cmp.Diff(tt.want, got))
			}
		})
	}
}

func TestSolution(t *testing.T) {
	f, err := os.Open("testdata/test_solution.dat")
	if err != nil {
		t.Fatal(err)
	}

	want := []weather.Result{
		{Name: "Korostyshiv", Min: -15.2, Avg: -15.2, Max: -15.2},
		{Name: "Sebba", Min: 0, Avg: 23.3, Max: 67},
	}

	got, err := weather.Solution(context.Background(), f, defaultLogger)
	if err != nil {
		t.Errorf("did not expect an error, got %v", err)
	}

	if !cmp.Equal(want, got) {
		t.Error(cmp.Diff(want, got))
	}
}

func TestChunkifyFile(t *testing.T) {
	cases := []struct {
		filepath  string
		chunkSize int
		want      [][]byte
	}{
		{filepath: "testdata/test_1.dat", chunkSize: 12, want: [][]byte{[]byte("abcd\nefgh\n"), []byte("ijkl")}},
		{filepath: "testdata/test_2.dat", chunkSize: 1024, want: [][]byte{}},
	}
	for _, tt := range cases {
		t.Run(tt.filepath, func(t *testing.T) {
			f, err := os.Open(tt.filepath)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			outCh := make(chan []byte)

			wg := &sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()

				err = weather.ChunkifyFile(context.Background(), f, tt.chunkSize, []byte{'\n'}, outCh, defaultLogger)
				if err != nil {
					t.Errorf("did not expect an error, got %v", err)
				}
			}()

			go func() {
				wg.Wait()
				close(outCh)
			}()

			got := make([][]byte, 0)

			for chunk := range outCh {
				got = append(got, chunk)
			}

			if !cmp.Equal(tt.want, got) {
				t.Error(cmp.Diff(tt.want, got))
			}
		})
	}
}
