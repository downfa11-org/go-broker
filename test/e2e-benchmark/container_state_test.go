package e2e_benchmark

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBenchmarkContainerExitRejectsFailedOrIncompleteState(t *testing.T) {
	for _, test := range []struct {
		name, state  string
		done, failed bool
	}{
		{"success", `{"Status":"exited","ExitCode":0,"OOMKilled":false}`, true, false},
		{"nonzero exit", `{"Status":"exited","ExitCode":7,"OOMKilled":false}`, false, true},
		{"OOM despite zero exit", `{"Status":"exited","ExitCode":0,"OOMKilled":true}`, false, true},
		{"engine failure", `{"Status":"exited","ExitCode":0,"OOMKilled":false,"Error":"exec failed"}`, false, true},
		{"running", `{"Status":"running","ExitCode":0,"OOMKilled":false}`, false, false},
		{"created", `{"Status":"created","ExitCode":0,"OOMKilled":false}`, false, false},
		{"dead", `{"Status":"dead","ExitCode":0,"OOMKilled":false}`, false, true},
		{"missing code", `{"Status":"exited","OOMKilled":false}`, false, true},
		{"missing OOM", `{"Status":"exited","ExitCode":0}`, false, true},
		{"unknown status", `{"Status":"unexpected","ExitCode":0,"OOMKilled":false}`, false, true},
		{"null", `null`, false, true},
		{"invalid JSON", `exited`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			done, err := benchmarkContainerExit([]byte(test.state))
			require.Equal(t, test.done, done)
			require.Equal(t, test.failed, err != nil, "%v", err)
		})
	}
}
