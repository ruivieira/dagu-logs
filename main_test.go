package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFindDagFile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "yaml before separator", args: []string{"start", "pipeline.yaml", "--", "k=v"}, want: "pipeline.yaml"},
		{name: "yml extension", args: []string{"dag.yml"}, want: "dag.yml"},
		{name: "ignore after separator", args: []string{"start", "--", "file.yaml"}, want: ""},
		{name: "no yaml", args: []string{"start", "--", "k=v"}, want: ""},
		{name: "empty", args: nil, want: ""},
		{name: "first match wins", args: []string{"a.yaml", "b.yml"}, want: "a.yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := findDagFile(tt.args); got != tt.want {
				t.Fatalf("findDagFile(%v) = %q, want %q", tt.args, got, tt.want)
			}
		})
	}
}

func TestFormatLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "blank", raw: "  ", want: ""},
		{name: "null", raw: "null", want: ""},
		{name: "status only", raw: `{"status":"ok"}`, want: "ok"},
		{name: "plain text", raw: "hello world", want: "hello world"},
		{name: "invalid json object", raw: "{not-json", want: "{not-json"},
		{name: "multi key", raw: `{"a":1,"b":"x"}`, want: ""}, // order not guaranteed; checked below
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := formatLine(tt.raw)
			if tt.name == "multi key" {
				if !strings.Contains(got, "a: 1") || !strings.Contains(got, "b: x") {
					t.Fatalf("formatLine(%q) = %q, want both keys", tt.raw, got)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("formatLine(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseDagFile(t *testing.T) {
	t.Parallel()

	t.Run("valid yaml", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "my-dag.yaml")
		content := "name: cool-pipeline\nsteps:\n  - name: build\n    id: build\n  - name: test\n    id: test\n"
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		name, steps := parseDagFile(path)
		if name != "cool-pipeline" {
			t.Fatalf("name = %q, want cool-pipeline", name)
		}
		if len(steps) != 2 {
			t.Fatalf("steps = %d, want 2", len(steps))
		}
		if steps[0].Name != "build" || steps[1].ID != "test" {
			t.Fatalf("unexpected steps: %+v", steps)
		}
	})

	t.Run("empty name uses basename", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "fallback-name.yaml")
		if err := os.WriteFile(path, []byte("steps: []\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		name, steps := parseDagFile(path)
		if name != "fallback-name" {
			t.Fatalf("name = %q, want fallback-name", name)
		}
		if steps == nil {
			t.Fatal("expected non-nil steps slice")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		name, steps := parseDagFile(filepath.Join(t.TempDir(), "gone.yaml"))
		if name != "gone" {
			t.Fatalf("name = %q, want gone", name)
		}
		if steps != nil {
			t.Fatalf("steps = %v, want nil", steps)
		}
	})
}

func TestResolveRelativePaths(t *testing.T) {
	t.Parallel()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	// Relative values before -- must be left alone; only post-separator values resolve.
	in := []string{"start", "rel=./before.yaml", "dag.yaml", "--", "abs=/tmp/x", "rel=./subdir", "dot=.", "plain"}
	got := resolveRelativePaths(in)

	if got[1] != "rel=./before.yaml" {
		t.Fatalf("pre-separator arg rewritten: %q", got[1])
	}
	if got[4] != "abs=/tmp/x" {
		t.Fatalf("absolute path changed: %q", got[4])
	}
	wantRel := "rel=" + filepath.Join(cwd, "subdir")
	if got[5] != wantRel {
		t.Fatalf("rel = %q, want %q", got[5], wantRel)
	}
	wantDot := "dot=" + cwd
	if got[6] != wantDot {
		t.Fatalf("dot = %q, want %q", got[6], wantDot)
	}
	if got[7] != "plain" {
		t.Fatalf("non key=value changed: %q", got[7])
	}
}

func TestWaitForNewestDir(t *testing.T) {
	t.Parallel()

	t.Run("finds matching dir", func(t *testing.T) {
		t.Parallel()
		parent := t.TempDir()
		after := time.Now().Add(-time.Second)
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = os.Mkdir(filepath.Join(parent, "run_abc"), 0o755)
		}()
		got, err := waitForNewestDir(parent, "run_", after, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if filepath.Base(got) != "run_abc" {
			t.Fatalf("got %q, want run_abc", got)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()
		parent := t.TempDir()
		_, err := waitForNewestDir(parent, "run_", time.Now(), 400*time.Millisecond)
		if err == nil {
			t.Fatal("expected timeout error")
		}
		if !strings.Contains(err.Error(), "timeout") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
