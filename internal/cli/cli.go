// Package cli implements the oap-backup subcommands that talk to a running
// oap-backup service over its HTTP API: list, describe and run.
package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"oap-backup/internal/api"
	"oap-backup/internal/scheduler"
)

// Exit codes.
const (
	ExitOK     = 0
	ExitFailed = 1
	ExitUsage  = 2
)

// ServerEnv names the environment variable with the default --server.
const ServerEnv = "OAP_BACKUP_SERVER"

const defaultServer = "http://localhost:9090"

// Usage describes the CLI commands.
const Usage = usage

const usage = `Usage:
  oap-backup list [-o table|json]
      Backups that oap-backup detected: every pod with oap/backup-command,
      including the ones skipped because of invalid annotations.

  oap-backup describe <namespace>/<pod> [-o table|json]
      Everything about one backup: annotations, schedule, last run and the
      files on the backup storage.

  oap-backup status [--lines 15] [-o table|json]
      What oap-backup is doing now. For every backup in progress: how long
      it has been running (or that it is queued), its settings and the last
      lines of its log.

  oap-backup refresh [-o table|json]
      Rescan the pods now, instead of waiting for the next periodic scan
      (--refresh of the service), and list the result. Use it after you
      changed a pod's oap/backup-* annotations.

  oap-backup run <namespace>/<pod> [--wait] [--timeout 30m]
  oap-backup run --all [--wait] [--timeout 30m]
      Start a backup now. With --wait, print the backup's log while it
      runs, wait until it finishes and exit 1 if it failed.

Common flags:
  --server URL   address of the oap-backup service
                 (default $` + ServerEnv + `, else ` + defaultServer + `)

Without a command, oap-backup runs the service; see oap-backup --help.
`

// IsCommand reports whether arg is a CLI subcommand (as opposed to a flag of
// the service).
func IsCommand(arg string) bool {
	switch arg {
	case "list", "describe", "run", "refresh", "status", "help":
		return true
	}
	return false
}

// App runs CLI commands. The zero value is not usable; see New.
type App struct {
	Getenv func(string) string
	Stdout io.Writer
	Stderr io.Writer
	HTTP   *http.Client
	// Now, PollInterval and QuietInterval are replaced in tests.
	Now          func() time.Time
	PollInterval time.Duration
	// QuietInterval is how long run --wait stays silent before it prints
	// that the backup is still running.
	QuietInterval time.Duration
}

// New returns an App that writes to stdout and stderr.
func New(getenv func(string) string, stdout, stderr io.Writer) *App {
	return &App{
		Getenv: getenv, Stdout: stdout, Stderr: stderr,
		HTTP:          &http.Client{Timeout: 2 * time.Minute},
		Now:           time.Now,
		PollInterval:  2 * time.Second,
		QuietInterval: 30 * time.Second,
	}
}

// Run executes args (the subcommand and its arguments) and returns the
// process exit code.
func (a *App) Run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(a.Stderr, usage)
		return ExitUsage
	}
	var err error
	switch args[0] {
	case "list":
		err = a.list("list", http.MethodGet, "/targets", args[1:])
	case "refresh":
		err = a.list("refresh", http.MethodPost, "/refresh", args[1:])
	case "status":
		err = a.status(args[1:])
	case "describe":
		err = a.describe(args[1:])
	case "run":
		err = a.run(args[1:])
	case "help":
		fmt.Fprint(a.Stdout, usage)
		return ExitOK
	default:
		err = usageError{fmt.Sprintf("unknown command %q", args[0])}
	}

	var ue usageError
	switch {
	case err == nil:
		return ExitOK
	case errors.Is(err, flag.ErrHelp):
		fmt.Fprint(a.Stdout, usage)
		return ExitOK
	case errors.As(err, &ue):
		fmt.Fprintf(a.Stderr, "oap-backup: %s\n\n%s", ue.msg, usage)
		return ExitUsage
	default:
		fmt.Fprintf(a.Stderr, "oap-backup: %v\n", err)
		return ExitFailed
	}
}

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

// options are the flags shared by the commands.
type options struct {
	server string
	output string
}

func (a *App) flagSet(name string, o *options, withOutput bool) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard) // errors are reported by Run, with the usage
	def := a.Getenv(ServerEnv)
	if def == "" {
		def = defaultServer
	}
	fs.StringVar(&o.server, "server", def, "")
	if withOutput {
		fs.StringVar(&o.output, "o", "table", "")
		fs.StringVar(&o.output, "output", "table", "")
	}
	return fs
}

// parse parses flags that may come before, between or after the positional
// arguments, and returns the positional ones.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, err
			}
			return nil, usageError{err.Error()}
		}
		if fs.NArg() == 0 {
			return positional, nil
		}
		positional = append(positional, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func checkOutput(o options) error {
	if o.output != "table" && o.output != "json" {
		return usageError{fmt.Sprintf("-o %q: must be table or json", o.output)}
	}
	return nil
}

// targetKey validates a "<namespace>/<pod>" argument.
func targetKey(arg string) (namespace, pod string, err error) {
	namespace, pod, ok := strings.Cut(arg, "/")
	if !ok || namespace == "" || pod == "" || strings.Contains(pod, "/") {
		return "", "", usageError{fmt.Sprintf("target %q: must be <namespace>/<pod>", arg)}
	}
	return namespace, pod, nil
}

// list implements the commands that print the targets: "list", and
// "refresh", which makes the service rescan the pods first.
func (a *App) list(name, method, path string, args []string) error {
	var o options
	positional, err := parse(a.flagSet(name, &o, true), args)
	if err != nil {
		return err
	}
	if len(positional) > 0 {
		return usageError{name + " takes no arguments"}
	}
	if err := checkOutput(o); err != nil {
		return err
	}
	var list api.TargetList
	if err := a.call(o.server, method, path, &list); err != nil {
		return err
	}
	if o.output == "json" {
		return writeJSON(a.Stdout, list)
	}
	renderList(a.Stdout, list, a.Now())
	return nil
}

func (a *App) describe(args []string) error {
	var o options
	positional, err := parse(a.flagSet("describe", &o, true), args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return usageError{"describe takes exactly one <namespace>/<pod>"}
	}
	namespace, pod, err := targetKey(positional[0])
	if err != nil {
		return err
	}
	if err := checkOutput(o); err != nil {
		return err
	}
	var detail api.TargetDetail
	path := "/targets/" + url.PathEscape(namespace) + "/" + url.PathEscape(pod)
	if err := a.call(o.server, http.MethodGet, path, &detail); err != nil {
		return err
	}
	if o.output == "json" {
		return writeJSON(a.Stdout, detail)
	}
	renderDetail(a.Stdout, detail, a.Now())
	return nil
}

func (a *App) run(args []string) error {
	var o options
	var all, wait bool
	var timeout time.Duration
	fs := a.flagSet("run", &o, false)
	fs.BoolVar(&all, "all", false, "")
	fs.BoolVar(&wait, "wait", false, "")
	fs.DurationVar(&timeout, "timeout", 0, "")
	positional, err := parse(fs, args)
	if err != nil {
		return err
	}
	target := ""
	switch {
	case all && len(positional) > 0:
		return usageError{"run takes either <namespace>/<pod> or --all, not both"}
	case !all && len(positional) != 1:
		return usageError{"run takes exactly one <namespace>/<pod>, or --all"}
	case !all:
		if _, _, err := targetKey(positional[0]); err != nil {
			return err
		}
		target = positional[0]
	}
	if timeout < 0 {
		return usageError{"--timeout must not be negative"}
	}

	// The runs that finished before this one, to tell them from its result.
	before := map[string]int64{}
	if wait {
		var list api.TargetList
		if err := a.call(o.server, http.MethodGet, "/targets", &list); err != nil {
			return err
		}
		for _, t := range list.Targets {
			if t.LastRun != nil {
				before[t.Target.Key()] = t.LastRun.Seq
			}
		}
	}

	var resp api.RunResponse
	if err := a.call(o.server, http.MethodPost, "/run?target="+url.QueryEscape(target), &resp); err != nil {
		return err
	}
	if len(resp.Started) == 0 {
		fmt.Fprintln(a.Stdout, "No backup started: nothing is scheduled, or every backup is already running.")
		return nil
	}
	for _, key := range resp.Started {
		fmt.Fprintf(a.Stdout, "started backup of %s\n", key)
	}
	if !wait {
		fmt.Fprintln(a.Stdout, "The backup runs in the background. Add --wait to follow its log and get its result,")
		fmt.Fprintln(a.Stdout, "or check it with: oap-backup status")
		return nil
	}
	return a.wait(o.server, resp.Started, before, resp.LogAfter, timeout)
}

// wait polls until every started backup has finished. Meanwhile it prints
// the log lines the service records for them (cursors holds, per target,
// the log cursor from before the run), then each result. It returns an
// error if any backup failed.
func (a *App) wait(server string, started []string, before map[string]int64, cursors map[string]int64, timeout time.Duration) error {
	pending := map[string]bool{}
	for _, key := range started {
		pending[key] = true
	}
	if cursors == nil {
		cursors = map[string]int64{}
	}
	startedAt := a.Now()
	lastOutput := startedAt
	deadline := a.Now().Add(timeout)
	failed := 0
	for {
		var list api.TargetList
		if err := a.call(server, http.MethodGet, "/targets", &list); err != nil {
			return err
		}
		statuses := map[string]scheduler.TargetStatus{}
		for _, t := range list.Targets {
			statuses[t.Target.Key()] = t
		}
		for _, key := range started {
			if !pending[key] {
				continue
			}
			// The log is read after the status: when the status says
			// "finished", the log already has the run's last line.
			if a.printLog(server, key, cursors, len(started) > 1) {
				lastOutput = a.Now()
			}
			st, ok := statuses[key]
			switch {
			case !ok:
				fmt.Fprintf(a.Stdout, "%s: no longer scheduled, result unknown\n", key)
				failed++
			case st.Running:
				continue
			case st.LastRun == nil || st.LastRun.Seq == before[key]:
				fmt.Fprintf(a.Stdout, "%s: did not run (oap-backup is shutting down?)\n", key)
				failed++
			case st.LastRun.Error != "":
				fmt.Fprintf(a.Stdout, "%s: failed after %s: %s\n", key, runDuration(st.LastRun), st.LastRun.Error)
				failed++
			default:
				fmt.Fprintf(a.Stdout, "%s: ok in %s, %s uploaded\n", key, runDuration(st.LastRun), uploaded(st.LastRun))
			}
			delete(pending, key)
		}
		if len(pending) == 0 {
			break
		}
		if timeout > 0 && !a.Now().Before(deadline) {
			return fmt.Errorf("timed out after %s; still running: %s", timeout, strings.Join(sortedKeys(pending), ", "))
		}
		// A sign of life while the backup command produces no log lines.
		if now := a.Now(); now.Sub(lastOutput) >= a.QuietInterval {
			fmt.Fprintf(a.Stdout, "... still running after %s: %s\n",
				shortDuration(now.Sub(startedAt)), strings.Join(sortedKeys(pending), ", "))
			lastOutput = now
		}
		time.Sleep(a.PollInterval)
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d backups did not succeed", failed, len(started))
	}
	return nil
}

// printLog prints the lines the service recorded for target since
// cursors[target], advances the cursor, and reports whether it printed
// anything. Failing to read the log does not fail the wait.
func (a *App) printLog(server, target string, cursors map[string]int64, prefix bool) bool {
	namespace, pod, _ := strings.Cut(target, "/")
	path := fmt.Sprintf("/targets/%s/%s/log?after=%d", url.PathEscape(namespace), url.PathEscape(pod), cursors[target])
	var resp api.LogResponse
	if err := a.call(server, http.MethodGet, path, &resp); err != nil {
		return false
	}
	cursors[target] = resp.Next
	for _, line := range resp.Lines {
		if prefix {
			fmt.Fprintf(a.Stdout, "[%s] ", target)
		}
		fmt.Fprintf(a.Stdout, "  %s\n", formatLogLine(line))
	}
	return len(resp.Lines) > 0
}

// call sends a request to the service and decodes a 2xx JSON response into
// out. Any other response becomes an error with the server's message.
func (a *App) call(server, method, path string, out any) error {
	base := strings.TrimRight(server, "/")
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	req, err := http.NewRequest(method, base+path, nil)
	if err != nil {
		return usageError{fmt.Sprintf("--server %q: %v", server, err)}
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach oap-backup at %s: %w", base, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("reading response from %s: %w", base, err)
	}
	if resp.StatusCode/100 != 2 {
		var e api.ErrorResponse
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return errors.New(e.Error)
		}
		return fmt.Errorf("%s %s%s: %s", method, base, path, resp.Status)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unexpected response from %s%s (is it an oap-backup service?): %w", base, path, err)
	}
	return nil
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
