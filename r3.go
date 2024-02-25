// r3: parse temperatures manually instead of using strconv.ParseFloat
//
// ~517ms for 10M rows (1.94x as fast as r1)

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
)

func r3(inputPath string, output io.Writer) error {
	type stats struct {
		min, max, sum float64
		count         int64
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
		temp := float64(tempBytes[index] - '0')
		index++
		if tempBytes[index] != '.' {
			temp = temp*10 + float64(tempBytes[index]-'0')
			index++
		}
		index++ // skip '.'
		temp += float64(tempBytes[index]-'0') / 10
		if negative {
			temp = -temp
		}

		s := stationStats[string(station)]
		if s == nil {
			stationStats[string(station)] = &stats{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
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
		mean := s.sum / float64(s.count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, s.min, mean, s.max)
	}
	fmt.Fprint(output, "}\n")
	return nil
}
