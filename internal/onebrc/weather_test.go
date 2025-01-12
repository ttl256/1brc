package onebrc_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/ttl256/1brcgo/internal/onebrc"
)

var defaultLogger = slog.New( //nolint: gochecknoglobals // let me be
	slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}), //nolint: exhaustruct // let me be
)

func TestParseRow(t *testing.T) {
	cases := []struct {
		input string
		want  onebrc.Measurement
	}{
		{input: "Sharya;92.4", want: onebrc.Measurement{Name: "Sharya", Temperature: 92.4}},
		{input: "Đà Nẵng;17.0", want: onebrc.Measurement{Name: "Đà Nẵng", Temperature: 17.0}},
		{input: "Saint-Saulve;-40.3", want: onebrc.Measurement{Name: "Saint-Saulve", Temperature: -40.3}},
	}

	for _, tt := range cases {
		t.Run(tt.input, func(t *testing.T) {
			got, err := onebrc.ParseRow(tt.input)
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

	want := onebrc.NewCityResults([]onebrc.Result{
		{Name: "Korostyshiv", Min: -15.2, Avg: -15.2, Max: -15.2},
		{Name: "Sebba", Min: 0, Avg: 23.4, Max: 67},
	})

	got, err := onebrc.Solution(context.Background(), f, defaultLogger)
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

				err = onebrc.ChunkifyFile(context.Background(), f, tt.chunkSize, []byte{'\n'}, outCh, defaultLogger)
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

func TestBuildCityString(t *testing.T) {
	input := []onebrc.Result{
		{Name: "C", Min: -10.1, Avg: 0, Max: 98.9},
		{Name: "B", Min: 0, Avg: 0, Max: 0},
		{Name: "A", Min: -99.9, Avg: 0.9, Max: 99.9},
	}

	want := "{C=-10.1/0.0/98.9, B=0.0/0.0/0.0, A=-99.9/0.9/99.9}"

	got := onebrc.NewCityResults(input).String()

	if !cmp.Equal(want, got) {
		t.Error(cmp.Diff(want, got))
	}
}
