// r4: use fixed point int32s (*10) instead of float64s
//
// ~491ms for 10M rows (2.04x as fast as r1)

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
)

func r4(inputPath string, output io.Writer) error {
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
		station, tempBytes, hasSemi := bytes.Cut(line, []byte(";"))
		if !hasSemi {
			continue
		}

		negative := false
		index := 0
		if tempBytes[index] == '-' {
			index++
			negative = true
		}
		temp := int32(tempBytes[index] - '0')
		index++
		if tempBytes[index] != '.' {
			temp = temp*10 + int32(tempBytes[index]-'0')
			index++
		}
		index++ // skip '.'
		temp = temp*10 + int32(tempBytes[index]-'0')
		if negative {
			temp = -temp
		}

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
