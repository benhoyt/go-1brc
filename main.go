package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

type revisionFunc func(string, io.Writer) error

var revisionFuncs = []revisionFunc{r1, r2, r3, r4, r5, r6, r7, r8, r9}

var maxGoroutines int

func main() {
	var (
		cpuProfile = flag.String("cpuprofile", "", "write CPU profile to file")
		revision   = flag.Int("revision", 1, "revision of solution to run")
		goroutines = flag.Int("goroutines", 0, "num goroutines for parallel solutions (default NumCPU)")
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
