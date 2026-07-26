package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pterm/pterm"
	"gopkg.in/yaml.v3"
)

var palette = []pterm.Color{
	pterm.FgCyan,
	pterm.FgGreen,
	pterm.FgYellow,
	pterm.FgMagenta,
	pterm.FgLightBlue,
	pterm.FgLightCyan,
	pterm.FgLightGreen,
	pterm.FgLightMagenta,
}

type stepYAML struct {
	Name   string `yaml:"name"`
	ID     string `yaml:"id"`
	Action string `yaml:"action"`
}

type dagYAML struct {
	Name  string     `yaml:"name"`
	Steps []stepYAML `yaml:"steps"`
}

func parseDagFile(file string) (name string, steps []stepYAML) {
	abs, _ := filepath.Abs(file)
	data, err := os.ReadFile(abs)
	base := filepath.Base(file)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if err != nil {
		return base, nil
	}
	var d dagYAML
	if yaml.Unmarshal(data, &d) != nil {
		return base, nil
	}
	n := d.Name
	if n == "" {
		n = base
	}
	return n, d.Steps
}

func findDagFile(args []string) string {
	for _, a := range args {
		if a == "--" {
			break
		}
		if strings.HasSuffix(a, ".yaml") || strings.HasSuffix(a, ".yml") {
			return a
		}
	}
	return ""
}

func waitForNewestDir(parent, prefix string, after time.Time, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(parent)
		if err == nil {
			var best string
			var bestTime time.Time
			for _, e := range entries {
				if !e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
					continue
				}
				info, err := e.Info()
				if err != nil {
					continue
				}
				if info.ModTime().After(after) && info.ModTime().After(bestTime) {
					bestTime = info.ModTime()
					best = filepath.Join(parent, e.Name())
				}
			}
			if best != "" {
				return best, nil
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return "", fmt.Errorf("timeout waiting for %s* in %s", prefix, parent)
}

func formatLine(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var obj map[string]any
		if err := json.Unmarshal([]byte(trimmed), &obj); err == nil {
			if len(obj) == 1 {
				if v, ok := obj["status"]; ok {
					return fmt.Sprintf("%v", v)
				}
			}
			parts := make([]string, 0, len(obj))
			for k, v := range obj {
				parts = append(parts, fmt.Sprintf("%s: %v", k, v))
			}
			return strings.Join(parts, "  ")
		}
	}
	return trimmed
}

var visibleStepCounter atomic.Int32

// tailFile tails a single step log file.
// If showHeartbeat is false the "running..." pulse is suppressed (used for
// sub-DAG .err files that may simply be empty).
func tailFile(path, stepName string, color pterm.Color, totalSteps int, showHeartbeat bool, wg *sync.WaitGroup, stop <-chan struct{}) {
	defer wg.Done()

	label := pterm.NewStyle(color, pterm.Bold).Sprintf("%-22s", stepName)
	sep := pterm.NewStyle(pterm.FgGray).Sprint(" │ ")
	gray := pterm.NewStyle(pterm.FgGray)

	const progressWidth = 8
	progressTag := func(n int) string {
		var s string
		if totalSteps > 0 {
			s = fmt.Sprintf("(%d/%d)", n, totalSteps)
		} else {
			s = fmt.Sprintf("(%d)", n)
		}
		return gray.Sprintf("%-*s ", progressWidth-1, s)
	}
	blank := strings.Repeat(" ", progressWidth)

	var f *os.File
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		f, err = os.Open(path)
		if err == nil {
			break
		}
		select {
		case <-stop:
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if f == nil {
		return
	}
	defer f.Close()

	stepNum := -1
	hasRealOutput := false
	shownRunning := false
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()

	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if line != "" {
			trimmed := strings.TrimSpace(line)
			if trimmed == "null" {
				if !hasRealOutput && stepNum != -1 {
					fmt.Println(blank + label + sep + gray.Sprint("done"))
				}
				return
			}
			if formatted := formatLine(line); formatted != "" {
				if stepNum == -1 {
					stepNum = int(visibleStepCounter.Add(1))
				}
				if !hasRealOutput {
					fmt.Println(progressTag(stepNum) + label + sep + formatted)
				} else {
					fmt.Println(blank + label + sep + formatted)
				}
				hasRealOutput = true
			}
		}
		if err != nil {
			select {
			case <-stop:
				return
			case <-heartbeat.C:
				if showHeartbeat && !hasRealOutput && !shownRunning {
					if stepNum == -1 {
						stepNum = int(visibleStepCounter.Add(1))
					}
					fmt.Println(progressTag(stepNum) + label + sep + gray.Sprint("running..."))
					shownRunning = true
				}
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
}

// watchStepLogs polls stepDir for new .out files (parent pipeline steps).
func watchStepLogs(stepDir string, totalSteps int, stop <-chan struct{}) {
	seen := map[string]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	colorIdx := 0

	tick := time.NewTicker(200 * time.Millisecond)
	defer func() {
		tick.Stop()
		wg.Wait()
	}()

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}

		entries, err := os.ReadDir(stepDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".out") {
				continue
			}
			mu.Lock()
			if seen[e.Name()] {
				mu.Unlock()
				continue
			}
			seen[e.Name()] = true
			color := palette[colorIdx%len(palette)]
			colorIdx++
			mu.Unlock()

			stepName := strings.SplitN(e.Name(), ".", 2)[0]
			wg.Add(1)
			go tailFile(filepath.Join(stepDir, e.Name()), stepName, color, totalSteps, true, &wg, stop)
		}
	}
}

// watchSubDagErrFiles watches for bare run_* directories appearing at logsBase
// after startedAt and tails their .err files for real-time sub-DAG step output.
// Dagu writes action step stdout to the parent step's .out (already covered by
// watchStepLogs), but stderr goes only into the sub-DAG's own run directory.
func watchSubDagErrFiles(logsBase string, startedAt time.Time, stop <-chan struct{}) {
	seenDirs := map[string]bool{}
	seenFiles := map[string]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	colorIdx := 0

	tick := time.NewTicker(500 * time.Millisecond)
	defer func() {
		tick.Stop()
		wg.Wait()
	}()

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}

		entries, err := os.ReadDir(logsBase)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() || !strings.HasPrefix(e.Name(), "run_") {
				continue
			}
			info, err := e.Info()
			if err != nil || !info.ModTime().After(startedAt.Add(-2*time.Second)) {
				continue
			}

			dirPath := filepath.Join(logsBase, e.Name())

			// Scan for new .err files in this sub-dag dir.
			subEntries, err := os.ReadDir(dirPath)
			if err != nil {
				continue
			}
			for _, sf := range subEntries {
				if sf.IsDir() || !strings.HasSuffix(sf.Name(), ".err") {
					continue
				}
				stepName := strings.SplitN(sf.Name(), ".", 2)[0]
				// Skip outputs.write steps — their .err is always empty noise.
				if stepName == "publish" {
					continue
				}

				fileKey := e.Name() + "/" + sf.Name()
				mu.Lock()
				if seenFiles[fileKey] {
					mu.Unlock()
					continue
				}
				if !seenDirs[e.Name()] {
					seenDirs[e.Name()] = true
				}
				seenFiles[fileKey] = true
				color := palette[(len(palette)/2+colorIdx)%len(palette)] // offset from parent palette
				colorIdx++
				mu.Unlock()

				wg.Add(1)
				go tailFile(filepath.Join(dirPath, sf.Name()), stepName, color, 0, false, &wg, stop)
			}
		}
	}
}

func resolveRelativePaths(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)
	sawSep := false
	for i, a := range out {
		if a == "--" {
			sawSep = true
			continue
		}
		if !sawSep {
			continue
		}
		k, v, ok := strings.Cut(a, "=")
		if !ok {
			continue
		}
		if v == "." || v == ".." || strings.HasPrefix(v, "./") || strings.HasPrefix(v, "../") {
			if abs, err := filepath.Abs(v); err == nil {
				out[i] = k + "=" + abs
				fmt.Fprintf(os.Stderr, "dagu-logs: resolved %s=. → %s\n", k, abs)
			}
		}
	}
	return out
}

func passthrough(args []string) {
	cmd := exec.Command("dagu", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		os.Exit(1)
	}
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		passthrough(args)
		return
	}

	if args[0] != "start" {
		passthrough(args)
		return
	}

	dagFile := findDagFile(args[1:])
	if dagFile == "" {
		passthrough(args)
		return
	}

	args = resolveRelativePaths(args)

	name, steps := parseDagFile(dagFile)
	totalSteps := len(steps)
	logsBase := filepath.Join(os.Getenv("HOME"), ".local", "share", "dagu", "logs")

	cmd := exec.Command("dagu", args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout // direct: dagu writes tree at end only; colors preserved
	cmd.Stderr = io.Discard // suppress spinner (fixes alignment)

	startedAt := time.Now()
	if err := cmd.Start(); err != nil {
		pterm.Error.Println("failed to start dagu:", err)
		os.Exit(1)
	}

	pterm.DefaultSection.Println(name)

	stop := make(chan struct{})
	var once sync.Once
	closeStop := func() { once.Do(func() { close(stop) }) }

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; closeStop() }()

	go func() {
		dagRunDir, err := waitForNewestDir(
			filepath.Join(logsBase, name), "dag-run_", startedAt.Add(-time.Second), 30*time.Second,
		)
		if err != nil {
			pterm.Warning.Println(err)
			return
		}
		stepDir, err := waitForNewestDir(dagRunDir, "run_", startedAt.Add(-time.Second), 30*time.Second)
		if err != nil {
			pterm.Warning.Println(err)
			return
		}
		watchStepLogs(stepDir, totalSteps, stop)
	}()

	// Watch sub-DAG stderr in parallel — action steps write stderr only to
	// their own bare run_* directory at the logs root (not forwarded to parent).
	go watchSubDagErrFiles(logsBase, startedAt, stop)

	cmd.Wait()
	time.Sleep(500 * time.Millisecond)
	closeStop()
	time.Sleep(200 * time.Millisecond)
}
