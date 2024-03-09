// r7: use custom hash table and hash station name as we look for ';'
//
// ~234ms for 10M rows (4.29x as fast as r1)

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
)

func r7(inputPath string, output io.Writer) error {
	type stats struct {
		min, max, count int32
		sum             int64
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	type item struct {
		key  []byte
		stat *stats
	}
	const numBuckets = 1 << 17        // number of hash buckets (power of 2)
	items := make([]item, numBuckets) // hash buckets, linearly probed
	size := 0                         // number of active items in items slice

	buf := make([]byte, 1024*1024)
	readStart := 0
	for {
		n, err := f.Read(buf[readStart:])
		if err != nil && err != io.EOF {
			return err
		}
		if readStart+n == 0 {
			break
		}
		chunk := buf[:readStart+n]

		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			break
		}
		remaining := chunk[newline+1:]
		chunk = chunk[:newline+1]

		for {
			const (
				// FNV-1 64-bit constants from hash/fnv.
				offset64 = 14695981039346656037
				prime64  = 1099511628211
			)

			var station, after []byte
			hash := uint64(offset64)
			i := 0
			for ; i < len(chunk); i++ {
				c := chunk[i]
				if c == ';' {
					station = chunk[:i]
					after = chunk[i+1:]
					break
				}
				hash ^= uint64(c) // FNV-1a is XOR then *
				hash *= prime64
			}
			if i == len(chunk) {
				break
			}

			index := 0
			negative := false
			if after[index] == '-' {
				negative = true
				index++
			}
			temp := int32(after[index] - '0')
			index++
			if after[index] != '.' {
				temp = temp*10 + int32(after[index]-'0')
				index++
			}
			index++ // skip '.'
			temp = temp*10 + int32(after[index]-'0')
			index += 2 // skip last digit and '\n'
			if negative {
				temp = -temp
			}
			chunk = after[index:]

			hashIndex := int(hash & uint64(numBuckets-1))
			for {
				if items[hashIndex].key == nil {
					// Found empty slot, add new item (copying key).
					key := make([]byte, len(station))
					copy(key, station)
					items[hashIndex] = item{
						key: key,
						stat: &stats{
							min:   temp,
							max:   temp,
							sum:   int64(temp),
							count: 1,
						},
					}
					size++
					if size > numBuckets/2 {
						panic("too many items in hash table")
					}
					break
				}
				if bytes.Equal(items[hashIndex].key, station) {
					// Found matching slot, add to existing stats.
					s := items[hashIndex].stat
					s.min = min(s.min, temp)
					s.max = max(s.max, temp)
					s.sum += int64(temp)
					s.count++
					break
				}
				// Slot already holds another key, try next slot (linear probe).
				hashIndex++
				if hashIndex >= numBuckets {
					hashIndex = 0
				}
			}
		}

		readStart = copy(buf, remaining)
	}

	stationItems := make([]item, 0, size)
	for _, item := range items {
		if item.key == nil {
			continue
		}
		stationItems = append(stationItems, item)
	}
	sort.Slice(stationItems, func(i, j int) bool {
		return string(stationItems[i].key) < string(stationItems[j].key)
	})

	fmt.Fprint(output, "{")
	for i, item := range stationItems {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := item.stat
		mean := float64(s.sum) / float64(s.count) / 10
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", item.key, float64(s.min)/10, mean, float64(s.max)/10)
	}
	fmt.Fprint(output, "}\n")
	return nil
}
