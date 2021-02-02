package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

//go:generate go run loader.go

type run struct {
	opsSec     float64
	readBytes  int64
	writeBytes int64
	readAmp    float64
	writeAmp   float64
}

type workload struct {
	days map[string][]run // data -> runs
}

type loader struct {
	data map[string]*workload // workload name -> data
}

func (l *loader) load(path string) {
	// fmt.Println("loader", path)
	parts := strings.Split(path, string(os.PathSeparator))
	if len(parts) < 2 {
		return
	}
	day := parts[1]

	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		return
	}
	defer f.Close()

	// z := bzip2.NewReader(f)
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		// fmt.Println(line)
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}

		var r run
		var name string
		var ops int64
		n, err := fmt.Sscanf(line,
			"Benchmark%s %d %f ops/sec %d read %d write %f r-amp %f w-amp",
			&name, &ops, &r.opsSec, &r.readBytes, &r.writeBytes, &r.readAmp, &r.writeAmp)
		if err != nil || n != 7 {
			fmt.Fprintf(os.Stderr, "%s: %v\n", s.Text(), err)
			continue
		}

		w := l.data[name]
		if w == nil {
			w = &workload{days: make(map[string][]run)}
			l.data[name] = w
		}
		w.days[day] = append(w.days[day], r)
	}
}

func (l *loader) cook() map[string]string {
	m := make(map[string]string)
	for name, workload := range l.data {
		fmt.Println(name)
		m[name] = l.cookWorkload(workload)
	}
	return m
}

func (l *loader) cookWorkload(w *workload) string {
	days := make([]string, 0, len(w.days))
	for day := range w.days {
		days = append(days, day)
	}
	sort.Strings(days)

	var buf bytes.Buffer
	for _, day := range days {
		fmt.Fprintf(&buf, "%s,%s\n", day, l.cookDay(w.days[day]))
	}
	return buf.String()
}

func (l *loader) cookDay(runs []run) string {
	var sum float64
	for i := range runs {
		sum += runs[i].opsSec
	}
	mean := sum / float64(len(runs))

	var sum2 float64
	for i := range runs {
		v := runs[i].opsSec - mean
		sum2 += v * v
	}

	stddev := math.Sqrt(sum2 / float64(len(runs)))
	lo := mean - stddev
	hi := mean + stddev

	var avg run
	var count int
	for i := range runs {
		r := &runs[i]
		if r.opsSec < lo || r.opsSec > hi {
			continue
		}
		count++
		avg.opsSec += r.opsSec
		avg.readBytes += r.readBytes
		avg.writeBytes += r.writeBytes
		avg.readAmp += r.readAmp
		avg.writeAmp += r.writeAmp
	}

	return fmt.Sprintf("%.1f,%d,%d,%.1f,%.1f",
		avg.opsSec/float64(count),
		avg.readBytes/int64(count),
		avg.writeBytes/int64(count),
		avg.readAmp/float64(count),
		avg.writeAmp/float64(count))
}

func prettyJSON(v interface{}) []byte {
	data, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
	return data
}

func generateJS(cmd *cobra.Command, args []string) error {
	dir := args[0]
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
	fmt.Println("Generating JS file.")
	l := &loader{data: make(map[string]*workload)}
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			fmt.Println(path)
			l.load(path)
		}
		return nil
	})

	const output = "dashboard/js/data.js"
	content := append([]byte("data = ")[:], prettyJSON(l.cook())...)
	err := ioutil.WriteFile(output, content, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
	return nil
}
