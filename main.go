package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

type revisionFunc func(string, io.Writer) error

var revisionFuncs = []revisionFunc{r1, r2, r3, r4, r5, r6, r7, r8, r9, r10}

var maxGoroutines int

func main() {
	var (
		cpuProfile = flag.String("cpuprofile", "", "write CPU profile to file")
		revision   = flag.Int("revision", len(revisionFuncs), "revision of solution to run")
		goroutines = flag.Int("goroutines", 0, "num goroutines for parallel solutions (default NumCPU)")
		benchAll   = flag.Bool("benchall", false, "benchmark all solutions")
	)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"Usage: go-r1bc [-cpuprofile=PROFILE] [-revision=N] INPUTFILE\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *revision < 1 || *revision > len(revisionFuncs) {
		fmt.Fprintf(os.Stderr, "invalid revision %d\n", *revision)
		os.Exit(1)
	}
	maxGoroutines = *goroutines
	if maxGoroutines == 0 {
		maxGoroutines = runtime.NumCPU()
	}

	args := flag.Args()
	if len(args) < 1 {
		flag.Usage()
		os.Exit(2)
	}
	inputPath := args[0]

	st, err := os.Stat(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	size := st.Size()

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *benchAll {
		err := benchmarkAll(inputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	start := time.Now()
	output := bufio.NewWriter(os.Stdout)

	rf := revisionFuncs[*revision-1]
	err = rf(inputPath, output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	output.Flush()
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "Processed %.1fMB in %s\n",
		float64(size)/(1024*1024), elapsed)
}

func benchmarkAll(inputPath string) error {
	const tries = 5

	var buf bytes.Buffer
	err := r1(inputPath, &buf)
	if err != nil {
		return err
	}
	expected := buf.String()

	var r1Best time.Duration

	for i, rf := range revisionFuncs {
		fmt.Fprintf(os.Stderr, "r%d: ", i+1)
		bestTime := time.Duration(math.MaxInt64)
		for try := 0; try < tries; try++ {
			var output bytes.Buffer
			start := time.Now()
			err := rf(inputPath, &output)
			if err != nil {
				return err
			}
			elapsed := time.Since(start)
			fmt.Fprintf(os.Stderr, "%v ", elapsed)
			bestTime = min(bestTime, elapsed)
			if i == 0 {
				r1Best = bestTime
			}

			if output.String() != expected {
				return fmt.Errorf("r%d didn't give correct result", i+1)
			}
		}
		fmt.Fprintf(os.Stderr, "- best: %v (%.2fx as fast as r1)\n",
			bestTime, float64(r1Best)/float64(bestTime))
	}
	return nil
}
