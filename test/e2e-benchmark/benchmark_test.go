package e2e_benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	benchmarkTimeout      = 5 * time.Minute
	composeCommandTimeout = 10 * time.Minute
	dockerCommandTimeout  = 30 * time.Second
)

var severeBenchmarkLogPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(^|[\s|])(?:panic:|\[fatal\]|fatal:|fatal error:|level=fatal)(\s|$)`),
	regexp.MustCompile(`(?i)benchmark incomplete`),
	regexp.MustCompile(`(?i)verify failed`),
}

func getComposeCommand() []string {
	if _, err := exec.LookPath("docker"); err == nil {
		return []string{"docker", "compose"}
	}
	return []string{"docker-compose"}
}

func composeFile(rel string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot resolve benchmark compose path")
	}
	return filepath.Join(filepath.Dir(file), rel)
}

func runCompose(args ...string) *exec.Cmd {
	return runComposeContext(context.Background(), args...)
}

func runComposeContext(ctx context.Context, args ...string) *exec.Cmd {
	base := getComposeCommand()
	fullArgs := append([]string{}, base[1:]...)
	fullArgs = append(fullArgs, "--project-name", "cursus-benchmark")
	fullArgs = append(fullArgs, args...)
	// #nosec G204,G702 -- executable and arguments are selected by this E2E test harness.
	cmd := exec.CommandContext(ctx, base[0], fullArgs...)
	return cmd
}

func TestRunComposePlacesProjectNameAfterComposeSubcommand(t *testing.T) {
	cmd := runCompose("-f", "docker-compose.yml", "up", "-d")
	args := cmd.Args[1:]

	if len(args) == 0 {
		t.Fatalf("expected compose arguments")
	}
	if args[0] == "compose" {
		if len(args) < 3 || args[1] != "--project-name" || args[2] != "cursus-benchmark" {
			t.Fatalf("docker compose project name must follow compose subcommand, got %v", args)
		}
		return
	}
	if args[0] != "--project-name" || len(args) < 2 || args[1] != "cursus-benchmark" {
		t.Fatalf("docker-compose project name must be first argument, got %v", args)
	}
}
func TestBenchmarkFailurePatternsAvoidBroadFatalMatches(t *testing.T) {
	benign := "bench-consumer | nonfatal retry message\nbench-broker | fatality is not a log level"
	for _, pattern := range severeBenchmarkLogPatterns {
		if pattern.MatchString(benign) {
			t.Fatalf("pattern %q matched benign log", pattern.String())
		}
	}

	severe := "bench-consumer | fatal error: benchmark worker aborted"
	matched := false
	for _, pattern := range severeBenchmarkLogPatterns {
		matched = matched || pattern.MatchString(severe)
	}
	if !matched {
		t.Fatal("expected severe fatal log to match")
	}
}

func TestBenchmarkContainerLabelsMatchSameRepoTestCompose(t *testing.T) {
	composePath := filepath.Join("repo", "test", "cluster", "docker-compose.yml")
	labels := strings.Join([]string{
		"e2e-cluster",
		filepath.ToSlash(filepath.Join("repo", "test", "e2e-cluster", "docker-compose.yml")),
		filepath.ToSlash(filepath.Join("repo", "test", "e2e-cluster")),
	}, "\n")

	if !benchmarkContainerLabelsMatch(labels, composePath) {
		t.Fatal("expected same-repo test compose labels to be removable for fixed benchmark names")
	}

	otherRepoLabels := strings.Join([]string{
		"e2e-cluster",
		filepath.ToSlash(filepath.Join("other", "test", "e2e-cluster", "docker-compose.yml")),
	}, "\n")
	if benchmarkContainerLabelsMatch(otherRepoLabels, composePath) {
		t.Fatal("did not expect another repository's fixed-name container to match")
	}
}
func TestBenchmarkCounterParsesMultiDigitFailures(t *testing.T) {
	logs := "bench-consumer | Message missing       : 12\nbench-consumer | Duplicate (Offset)    : 0"
	missing, ok := benchmarkCounter(logs, "Message missing")
	if !ok || missing != 12 {
		t.Fatalf("expected missing=12, got %d ok=%t", missing, ok)
	}

	dupOffset, ok := benchmarkCounter(logs, "Duplicate (Offset)")
	if !ok || dupOffset != 0 {
		t.Fatalf("expected duplicate offset=0, got %d ok=%t", dupOffset, ok)
	}
}

func composeDown(t *testing.T, file string) {
	// Fixed container_name values bypass compose project-name isolation. Remove
	// only containers that Docker labels as belonging to these benchmark compose files.
	for _, name := range fixedBenchmarkContainers(file) {
		removeFixedBenchmarkContainer(t, file, name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), composeCommandTimeout)
	cmd := runComposeContext(ctx, "-f", file, "rm", "-f", "-s", "-v")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Logf("compose rm warning: %v\n%s", err, string(output))
	}
	cancel()

	ctx, cancel = context.WithTimeout(context.Background(), composeCommandTimeout)
	cmd = runComposeContext(ctx, "-f", file, "down", "-v", "--remove-orphans")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Logf("compose down warning: %v\n%s", err, string(output))
	}
	cancel()
}

func fixedBenchmarkContainers(file string) []string {
	if strings.Contains(filepath.ToSlash(file), "/cluster/") {
		return []string{"broker-1", "broker-2", "broker-3", "broker-publisher", "broker-consumer", "prometheus"}
	}
	return []string{"bench-broker", "bench-publisher", "bench-consumer"}
}

func removeFixedBenchmarkContainer(t *testing.T, file, name string) {
	if !isFixedBenchmarkContainer(t, file, name) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()
	// #nosec G204 -- name is a fixed E2E container name selected by the test table.
	cmd := exec.CommandContext(ctx, "docker", "rm", "-f", "-v", name)
	if output, err := cmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
		t.Logf("docker rm warning for %s: %v\n%s", name, err, string(output))
	}
}

func isFixedBenchmarkContainer(t *testing.T, file, name string) bool {
	format := strings.Join([]string{
		`{{ index .Config.Labels "com.docker.compose.project" }}`,
		`{{ index .Config.Labels "com.docker.compose.project.config_files" }}`,
		`{{ index .Config.Labels "com.docker.compose.project.working_dir" }}`,
	}, "\n")
	ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
	defer cancel()
	// #nosec G204 -- format and name are fixed values owned by the E2E test harness.
	cmd := exec.CommandContext(ctx, "docker", "inspect", "-f", format, name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false
	}

	absFile, err := filepath.Abs(file)
	if err != nil {
		t.Logf("failed to resolve compose file %s: %v", file, err)
		return false
	}

	return benchmarkContainerLabelsMatch(string(output), absFile)
}

func benchmarkContainerLabelsMatch(labels, absFile string) bool {
	normalizedLabels := filepath.ToSlash(labels)
	expectedFile := filepath.ToSlash(absFile)
	expectedDir := filepath.ToSlash(filepath.Dir(absFile))
	testRoot := benchmarkTestRoot(filepath.Dir(absFile))

	if strings.Contains(normalizedLabels, "cursus-benchmark") ||
		strings.Contains(normalizedLabels, expectedFile) ||
		strings.Contains(normalizedLabels, expectedDir) {
		return true
	}
	if testRoot == "" {
		return false
	}

	// Cluster benchmark compose files use fixed container_name values that can
	// collide with other Cursus test compose stacks, such as test/e2e-cluster.
	return strings.Contains(normalizedLabels, filepath.ToSlash(testRoot)+"/")
}

func benchmarkTestRoot(dir string) string {
	for {
		if filepath.Base(dir) == "test" {
			return filepath.Clean(dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func composeUp(t *testing.T, file string) {
	ctx, cancel := context.WithTimeout(context.Background(), composeCommandTimeout)
	cmd := runComposeContext(ctx, "-f", file, "build", "--progress", "plain")
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		cancel()
		t.Fatalf("compose build timed out after %s\n%s", composeCommandTimeout, string(output))
	}
	cancel()
	if err != nil {
		t.Fatalf("compose build failed: %v\n%s", err, string(output))
	}

	ctx, cancel = context.WithTimeout(context.Background(), composeCommandTimeout)
	cmd = runComposeContext(ctx, "-f", file, "up", "-d", "--no-build", "--force-recreate")
	output, err = cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		cancel()
		t.Fatalf("compose up timed out after %s\n%s", composeCommandTimeout, string(output))
	}
	cancel()
	if err != nil {
		t.Fatalf("compose up failed: %v\n%s", err, string(output))
	}
}

func benchmarkContainerExit(data []byte) (bool, error) {
	var state struct {
		Status    string
		ExitCode  *int
		OOMKilled *bool
		Error     string
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return false, fmt.Errorf("decode container state: %w", err)
	}
	if state.ExitCode == nil || state.OOMKilled == nil {
		return false, fmt.Errorf("container state is missing exit code or OOM status")
	}
	if *state.OOMKilled || state.Error != "" {
		return false, fmt.Errorf("container failed: status=%s exit_code=%d oom_killed=%t error=%s", state.Status, *state.ExitCode, *state.OOMKilled, state.Error)
	}
	switch state.Status {
	case "exited":
		if *state.ExitCode != 0 {
			return false, fmt.Errorf("container exited with code %d", *state.ExitCode)
		}
		return true, nil
	case "created", "running", "paused", "restarting":
		return false, nil
	default:
		return false, fmt.Errorf("unexpected container state %q", state.Status)
	}
}

func waitForContainerExit(t *testing.T, file, service, container string, timeout time.Duration) (string, error) {
	waitCtx, stop := context.WithTimeout(context.Background(), timeout)
	defer stop()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	var lastInspectErr error

	for {
		select {
		case <-waitCtx.Done():
			// Dump logs for debugging (compose logs uses service name)
			ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
			logsCmd := runComposeContext(ctx, "-f", file, "logs", "--tail", "50", service)
			logs, _ := logsCmd.CombinedOutput()
			cancel()
			t.Logf("Timeout waiting for %s. Last logs:\n%s", container, string(logs))
			return string(logs), fmt.Errorf("waiting for %s: %w (last inspect error: %v)", container, waitCtx.Err(), lastInspectErr)
		case <-ticker.C:
			// Check if container has exited (docker inspect uses container name)
			ctx, cancel := context.WithTimeout(waitCtx, dockerCommandTimeout)
			// #nosec G204 -- container is a fixed E2E container name selected by the caller.
			inspectCmd := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{json .State}}", container)
			out, err := inspectCmd.Output()
			cancel()
			if err != nil {
				lastInspectErr = err
				continue
			}
			lastInspectErr = nil
			completed, stateErr := benchmarkContainerExit(out)
			if completed || stateErr != nil {
				ctx, cancel := context.WithTimeout(context.Background(), dockerCommandTimeout)
				logsCmd := runComposeContext(ctx, "-f", file, "logs", service)
				logs, logErr := logsCmd.CombinedOutput()
				cancel()
				if stateErr != nil {
					return string(logs), fmt.Errorf("%s: %w", container, stateErr)
				}
				if logErr != nil {
					return string(logs), fmt.Errorf("read %s benchmark logs: %w", container, logErr)
				}
				return string(logs), nil
			}
		}
	}
}
func assertBenchmarkSuccess(t *testing.T, logs string, component string) {
	t.Helper()

	for _, pattern := range severeBenchmarkLogPatterns {
		if pattern.MatchString(logs) {
			t.Fatalf("%s benchmark reported failure marker %q:\n%s", component, pattern.String(), lastLines(logs, 40))
		}
	}

	for _, label := range []string{"Failed messages", "Message missing", "Duplicate (MessageID)", "Duplicate (Offset)"} {
		if count, ok := benchmarkCounter(logs, label); ok && count > 0 {
			t.Fatalf("%s benchmark reported %s=%d:\n%s", component, label, count, lastLines(logs, 40))
		}
	}

	switch component {
	case "Publisher":
		failed, ok := benchmarkCounter(logs, "Failed messages")
		if !ok || failed != 0 || !strings.Contains(logs, "rate: 100.00%") {
			t.Fatalf("publisher did not report a complete publish:\n%s", lastLines(logs, 40))
		}
	case "Consumer":
		if !strings.Contains(logs, "All messages consumed") {
			t.Fatalf("consumer did not report complete consumption:\n%s", lastLines(logs, 40))
		}
		for _, label := range []string{"Message missing", "Duplicate (MessageID)", "Duplicate (Offset)"} {
			count, ok := benchmarkCounter(logs, label)
			if !ok || count != 0 {
				t.Fatalf("consumer did not report %s=0:\n%s", label, lastLines(logs, 40))
			}
		}
	}

	summary := benchmarkResultSummary(logs)
	if summary != "" {
		t.Logf("%s benchmark result:\n%s", component, summary)
	} else {
		t.Logf("%s benchmark completed successfully", component)
	}
}

func benchmarkResultSummary(logs string) string {
	wanted := []string{
		"PRODUCER BENCHMARK SUMMARY",
		"CONSUMER BENCHMARK SUMMARY",
		"Partitions",
		"Total Batches",
		"Total Messages",
		"Failed messages",
		"Retry Count",
		"Publish elapsed Time",
		"Publish Message Throughput",
		"Latency P95",
		"Latency P99",
		"Elapsed Time",
		"Overall TPS",
		"Duplicate (MessageID)",
		"Duplicate (Offset)",
		"Message missing",
	}

	var out []string
	for _, line := range strings.Split(logs, "\n") {
		for _, token := range wanted {
			if strings.Contains(line, token) {
				out = append(out, strings.TrimSpace(line))
				break
			}
		}
	}
	return strings.Join(out, "\n")
}
func benchmarkCounter(logs, label string) (int, bool) {
	pattern := regexp.MustCompile(fmt.Sprintf(`(?im)%s[[:space:]]*:[[:space:]]*([0-9]+)`, regexp.QuoteMeta(label)))
	match := pattern.FindStringSubmatch(logs)
	if match == nil {
		return 0, false
	}
	count, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return count, true
}

func lastLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

// TestStandaloneBenchmark runs the standalone benchmark and verifies produce/consume complete.
func TestStandaloneBenchmark(t *testing.T) {
	if os.Getenv("RUN_E2E_BENCHMARK") != "1" {
		t.Skip("set RUN_E2E_BENCHMARK=1 to run docker-compose benchmark")
	}
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	composeDown(t, composeFile("../docker-compose.yml"))
	t.Cleanup(func() { composeDown(t, composeFile("../docker-compose.yml")) })

	t.Log("Starting standalone benchmark...")
	composeUp(t, composeFile("../docker-compose.yml"))

	// Wait for publisher to finish
	t.Log("Waiting for publisher to complete...")
	pubLogs, err := waitForContainerExit(t, composeFile("../docker-compose.yml"), "publisher", "bench-publisher", benchmarkTimeout)
	if err != nil {
		t.Fatalf("Publisher failed: %v\n%s", err, lastLines(pubLogs, 40))
	}
	assertBenchmarkSuccess(t, pubLogs, "Publisher")

	// Wait for consumer to finish
	t.Log("Waiting for consumer to complete...")
	consLogs, err := waitForContainerExit(t, composeFile("../docker-compose.yml"), "consumer", "bench-consumer", benchmarkTimeout)
	if err != nil {
		t.Fatalf("Consumer failed: %v\n%s", err, lastLines(consLogs, 40))
	}
	assertBenchmarkSuccess(t, consLogs, "Consumer")
}

// TestClusterBenchmark runs the 3-node cluster benchmark and verifies produce/consume complete.
func TestClusterBenchmark(t *testing.T) {
	if os.Getenv("RUN_E2E_BENCHMARK") != "1" {
		t.Skip("set RUN_E2E_BENCHMARK=1 to run docker-compose benchmark")
	}
	if testing.Short() {
		t.Skip("skipping benchmark test in short mode")
	}

	composeDown(t, composeFile("../cluster/docker-compose.yml"))
	t.Cleanup(func() { composeDown(t, composeFile("../cluster/docker-compose.yml")) })

	t.Log("Starting cluster benchmark (3 nodes)...")
	composeUp(t, composeFile("../cluster/docker-compose.yml"))

	// Wait for publisher to finish
	t.Log("Waiting for publisher to complete...")
	pubLogs, err := waitForContainerExit(t, composeFile("../cluster/docker-compose.yml"), "publisher", "broker-publisher", benchmarkTimeout)
	if err != nil {
		t.Fatalf("Publisher failed: %v\n%s", err, lastLines(pubLogs, 40))
	}
	assertBenchmarkSuccess(t, pubLogs, "Publisher")

	// Wait for consumer to finish
	t.Log("Waiting for consumer to complete...")
	consLogs, err := waitForContainerExit(t, composeFile("../cluster/docker-compose.yml"), "consumer", "broker-consumer", benchmarkTimeout)
	if err != nil {
		t.Fatalf("Consumer failed: %v\n%s", err, lastLines(consLogs, 40))
	}
	assertBenchmarkSuccess(t, consLogs, "Consumer")
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}
