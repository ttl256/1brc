package measurements

import (
	"context"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
)

const (
	keysetSize        = 10_000 // Number of unique weather statations in the dataset.
	letters           = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	oneDecimalPlace   = 10
	fourDecimalPlaces = 40
	minTemperature    = -99.0 // The challenge constraint.
	maxTemperature    = 99.0  // The challenge constraint.
	sigma             = 7.0   // A parameter for normal distribution used to generate average temperature.
	minLatitude       = -90
	maxLatitude       = 90
)

type WeatherStation struct {
	Name           string
	AvgTemperature float64
}

// Generates the challenge dataset using []string of capacity chunkSize sent
// over channel chunkCh.
func GenerateDataSet(ctx context.Context, r *rand.Rand, n int, chunkSize int, chunkCh chan<- []string) error {
	weatherStations := GenerateWeatherStations(r, keysetSize)

	buf := make([]string, 0, chunkSize)

	for range n {
		station := weatherStations[r.IntN(keysetSize)]
		// Get new temperature normally distributed around average temperature
		// for a given station
		temp := r.NormFloat64()*sigma + station.AvgTemperature
		// Clip just in case the values fell out of the range. Unlikely, but
		// possible nonetheless.
		temp = max(min(temp, maxTemperature), minTemperature)
		temp = math.Round(temp*oneDecimalPlace) / oneDecimalPlace

		buf = append(buf, station.Name+";"+strconv.FormatFloat(temp, 'f', 1, 64)+"\n")

		if len(buf) >= chunkSize {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case chunkCh <- buf:
			}
			buf = make([]string, 0, chunkSize)
		}
	}

	if len(buf) != 0 {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case chunkCh <- buf:
		}
	}

	return nil
}

// Generates a slice of [WeatherStation] of length n.
func GenerateWeatherStations(r *rand.Rand, n int) []WeatherStation {
	var stations []WeatherStation
	names := make(map[string]struct{})

	for range n {
		var name string

		for {
			name = generateStationName(r)
			if _, ok := names[name]; !ok {
				names[name] = struct{}{}
				break
			}
		}

		var station WeatherStation

		temp := generateAvgTemperature(r)

		station.Name = name
		station.AvgTemperature = temp

		stations = append(stations, station)
	}

	return stations
}

// Generates a station name with length in range [1;100]. Uses a 7th-order curve
// to simulate the name length distribution. It gives us mostly short names, but
// with large outliers.
func generateStationName(r *rand.Rand) string {
	const (
		yOffset = 4
		xOffset = 0.372
		factor  = 2500
		power   = 7
	)

	nameLen := int(yOffset + factor*math.Pow(r.Float64()-xOffset, power))

	buf := &strings.Builder{}
	buf.Grow(nameLen)

	for range nameLen {
		buf.WriteByte(letters[r.IntN(len(letters))])
	}

	return buf.String()
}

// Generates an average temperature. Latitude is chosen randomly, then the
// temperature is calculated using cosine of the latitude and rounded to one
// decimal place.
func generateAvgTemperature(r *rand.Rand) float64 {
	latitude := math.Round((minLatitude+r.Float64()*(maxLatitude-minLatitude))*fourDecimalPlaces) / fourDecimalPlaces
	avgTemp := 30*math.Cos(latitude*math.Pi/180) - 10 //nolint: mnd // fine

	return math.Round(avgTemp*oneDecimalPlace) / oneDecimalPlace
}
