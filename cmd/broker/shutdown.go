package main

import (
	"context"
	"time"

	"github.com/cursus-io/cursus/util"
)

// forceExit must terminate the process: in-flight filesystem calls cannot be canceled safely.
func superviseShutdown(ctx context.Context, timeout time.Duration, forceExit func(int), run func()) {
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-finished:
			return
		case <-ctx.Done():
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		go util.Warn("Shutdown requested; process exits unsuccessfully if cleanup exceeds %s", timeout)
		select {
		case <-finished:
			return
		case <-timer.C:
			select {
			case <-finished:
				return
			default:
			}
			forceExit(1)
		}
	}()
	run()
}

func onlyCancellation(err error) bool {
	if err == context.Canceled {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyCancellation(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyCancellation(wrapped.Unwrap())
	}
	return false
}
