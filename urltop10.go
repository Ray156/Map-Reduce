package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	// round 1: do url count
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	// round 2: sort and get the 10 most frequent URLs
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args

}

// URLCountMap is the map function in the first round
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvmap := make(map[string]int)
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		kvmap[l] += 1
	}
	for k, v := range kvmap {
		kvs = append(kvs, KeyValue{Key: k, Value: strconv.Itoa(v)})
	}
	return kvs
}

// URLCountReduce is the reduce function in the first round
func URLCountReduce(key string, values []string) string {
	var tot int
	for _, value := range values {
		number, _ := strconv.Atoi(value)
		tot = tot + number
	}

	return fmt.Sprintf("%s %d\n", key, tot)
}

// URLTop10Map is the map function in the second round
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		kvs = append(kvs, KeyValue{"", l})
	}
	return kvs
}

// URLTop10Reduce is the reduce function in the second round
func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		// println(v)
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] += n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
