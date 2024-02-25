// r5: avoid bytes.Cut
//
// ~442ms for 10M rows (2.27x as fast as r1)

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
)

func r5(inputPath string, output io.Writer) error {
	type stats struct {
		min, max, count int32
		sum             int64
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	stationStats := make(map[string]*stats)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()

		end := len(line)
		tenths := int32(line[end-1] - '0')
		ones := int32(line[end-3] - '0') // line[end-2] is '.'
		var temp int32
		var semicolon int
		if line[end-4] == ';' {
			temp = ones*10 + tenths
			semicolon = end - 4
		} else if line[end-4] == '-' {
			temp = -(ones*10 + tenths)
			semicolon = end - 5
		} else {
			tens := int32(line[end-4] - '0')
			if line[end-5] == ';' {
				temp = tens*100 + ones*10 + tenths
				semicolon = end - 5
			} else { // '-'
				temp = -(tens*100 + ones*10 + tenths)
				semicolon = end - 6
			}
		}

		station := line[:semicolon]
		s := stationStats[string(station)]
		if s == nil {
			stationStats[string(station)] = &stats{
				min:   temp,
				max:   temp,
				sum:   int64(temp),
				count: 1,
			}
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += int64(temp)
			s.count++
		}
	}

	stations := make([]string, 0, len(stationStats))
	for station := range stationStats {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Fprint(output, "{")
	for i, station := range stations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := stationStats[station]
		mean := float64(s.sum) / float64(s.count) / 10
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, float64(s.min)/10, mean, float64(s.max)/10)
	}
	fmt.Fprint(output, "}\n")
	return nil
}
