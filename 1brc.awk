BEGIN {
	FS = ";"
}

{
	if (counts[$1]++) {
		mins[$1] = $2 < mins[$1] ? $2 : mins[$1]
		maxs[$1] = $2 > maxs[$1] ? $2 : maxs[$1]
	} else {
		mins[$1] = maxs[$1] = $2  # new entry
	}
	sums[$1] += $2
}

END {
	printf "{"
	n = asorti(mins, sorted)
    for (i = 1; i <= n; i++) {
    	station = sorted[i]
		min = mins[station]
		max = maxs[station]
		mean = sums[station] / counts[station]
		printf "%s=%.1f/%.1f/%.1f", station, min, mean, max
		if (i < n) {
			printf ", "
		}
	}
	printf "}\n"
}
