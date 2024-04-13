// r10: all the previous optimizations plus faster semicolon finding and
// hashing
//
// Translated from Java by Menno Finlay-Smits Ideas with ideas taken from
// this fast Java solution:
//
// https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mtopolnik.java
//
// On my (Ben's) laptop I get these initial results:
//
// $ ./go-1brc -revision=1 ../1brc/data/measurements.txt >out-r1
// Processed 13156.2MB in 1m39.507011009s
// $ ./go-1brc -revision=9 ../1brc/data/measurements.txt >out-r9
// Processed 13156.2MB in 2.893693843s  # 34.4x as fast as the r1 above
// $ ./go-1brc -revision=10 ../1brc/data/measurements.txt >out-r10
// Processed 13156.2MB in 2.497241029s  # 39.8x as fast as the r1 above

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sort"
)

const BroadcastSemicolon = 0x3B3B3B3B3B3B3B3B
const Broadcast0x01 = 0x0101010101010101
const Broadcast0x80 = 0x8080808080808080

type r10Stats struct {
	min, max, count int32
	sum             int64
}

func r10(inputPath string, output io.Writer) error {
	parts, err := splitFile(inputPath, maxGoroutines)
	if err != nil {
		return err
	}

	resultsCh := make(chan map[string]*r10Stats)
	for _, part := range parts {
		go r10ProcessPart(inputPath, part.offset, part.size, resultsCh)
	}

	totals := make(map[string]*r10Stats)
	for i := 0; i < len(parts); i++ {
		result := <-resultsCh
		for station, s := range result {
			ts := totals[station]
			if ts == nil {
				totals[station] = s
				continue
			}
			ts.min = min(ts.min, s.min)
			ts.max = max(ts.max, s.max)
			ts.sum += s.sum
			ts.count += s.count
		}
	}

	stations := make([]string, 0, len(totals))
	for station := range totals {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Fprint(output, "{")
	for i, station := range stations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := totals[station]
		mean := float64(s.sum) / float64(s.count) / 10
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f", station, float64(s.min)/10, mean, float64(s.max)/10)
	}
	fmt.Fprint(output, "}\n")

	return nil
}

func r10ProcessPart(inputPath string, fileOffset, fileSize int64, resultsCh chan map[string]*r10Stats) {
	file, err := os.Open(inputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	f := io.LimitedReader{R: file, N: fileSize}

	type item struct {
		key  []byte
		stat *r10Stats
	}
	const numBuckets = 1 << 17        // number of hash buckets (power of 2)
	items := make([]item, numBuckets) // hash buckets, linearly probed
	size := 0                         // number of active items in items slice

	buf := make([]byte, 1024*1024)
	readStart := 0
	for {
		n, err := f.Read(buf[readStart:])
		if err != nil && err != io.EOF {
			panic(err)
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

	chunkLoop:
		for {
			var hash uint64
			var station, after []byte

			if len(chunk) < 8 {
				break chunkLoop
			}

			nameWord0 := binary.NativeEndian.Uint64(chunk)
			matchBits := semicolonMatchBits(nameWord0)
			if matchBits != 0 {
				// semicolon is in the first 8 bytes
				nameLen := calcNameLen(matchBits)
				nameWord0 = maskWord(nameWord0, matchBits)
				station = chunk[:nameLen]
				after = chunk[nameLen+1:]
				hash = calcHash(nameWord0)
			} else {
				// station name is longer so keep looking for the semicolon in
				// uint64 chunks
				nameLen := 8
				hash = calcHash(nameWord0)
				for {
					if nameLen > len(chunk)-8 {
						break chunkLoop
					}
					lastNameWord := binary.NativeEndian.Uint64(chunk[nameLen:])
					matchBits = semicolonMatchBits(lastNameWord)
					if matchBits != 0 {
						nameLen += calcNameLen(matchBits)
						station = chunk[:nameLen]
						after = chunk[nameLen+1:]
						break
					}
					nameLen += 8
				}
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

			hashIndex := int(hash & (numBuckets - 1))
			for {
				if items[hashIndex].key == nil {
					// Found empty slot, add new item (copying key).
					key := make([]byte, len(station))
					copy(key, station)
					items[hashIndex] = item{
						key: key,
						stat: &r10Stats{
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

	result := make(map[string]*r10Stats, size)
	for _, item := range items {
		if item.key == nil {
			continue
		}
		result[string(item.key)] = item.stat
	}
	resultsCh <- result
}

func calcNameLen(b uint64) int {
	return (bits.TrailingZeros64(b) >> 3)
}

func calcHash(word uint64) uint64 {
	return bits.RotateLeft64(word*0x51_7c_c1_b7_27_22_0a_95, 17)
}

func semicolonMatchBits(word uint64) uint64 {
	diff := word ^ BroadcastSemicolon
	return (diff - Broadcast0x01) & (^diff & Broadcast0x80)
}

func maskWord(word, matchBits uint64) uint64 {
	mask := matchBits ^ (matchBits - 1)
	return word & mask
}
