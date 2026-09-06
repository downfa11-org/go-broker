package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestOnlyCancellation(t *testing.T) {
	failure := errors.New("storage close failed")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"cancel", context.Canceled, true},
		{"wrapped", fmt.Errorf("server: %w", context.Canceled), true},
		{"joined cancellations", errors.Join(context.Canceled, context.Canceled), true},
		{"joined failure", errors.Join(context.Canceled, failure), false},
		{"wrapped joined failure", fmt.Errorf("broker: %w", errors.Join(context.Canceled, failure)), false},
		{"deadline", context.DeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := onlyCancellation(tc.err); got != tc.want {
				t.Fatalf("onlyCancellation(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestShutdownDeadlineSubprocess(t *testing.T) {
	if mode := os.Getenv("CURSUS_SHUTDOWN_TEST_MODE"); mode != "" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		superviseShutdown(ctx, 100*time.Millisecond, os.Exit, func() {
			cancel()
			if mode == "hung" {
				select {}
			}
			if mode == "failure" && !onlyCancellation(errors.Join(context.Canceled, errors.New("storage close failed"))) {
				os.Exit(1)
			}
		})
		// A completed run must disarm the deadline even after cancellation.
		time.Sleep(300 * time.Millisecond)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"hung", "clean", "failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestShutdownDeadlineSubprocess$")
			cmd.Env = append(os.Environ(), "CURSUS_SHUTDOWN_TEST_MODE="+mode)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("child exceeded parent deadline: %s", output)
			}
			if mode == "clean" {
				if err != nil {
					t.Fatalf("clean shutdown: %v: %s", err, output)
				}
				return
			}
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("expected exit 1, got %v: %s", err, output)
			}
		})
	}
}
