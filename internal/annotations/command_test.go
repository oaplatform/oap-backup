package annotations

import (
	"strings"
	"testing"
)

func TestParseCommandFromConfigMapOrSecret(t *testing.T) {
	for kind, key := range map[string]string{RefConfigMap: CommandConfigMap, RefSecret: CommandSecret} {
		ann := valid()
		delete(ann, Command)
		ann[key] = " pg-scripts "
		ann[CommandField] = " backup.sh "
		tg, ok, err := Parse(pod(ann, "postgres"), "@daily")
		if !ok || err != nil {
			t.Fatalf("%s: ok=%v err=%v", kind, ok, err)
		}
		want := Ref{Kind: kind, Name: "pg-scripts", Field: "backup.sh"}
		if tg.Command != "" || tg.CommandFrom != want {
			t.Errorf("%s: command=%q from=%+v, want %+v", kind, tg.Command, tg.CommandFrom, want)
		}
		if got, want := tg.CommandDisplay(), kind+" pg-scripts/backup.sh"; got != want {
			t.Errorf("CommandDisplay = %q, want %q", got, want)
		}
	}

	// Inline stays as it was.
	tg, _, err := Parse(pod(valid(), "postgres"), "@daily")
	if err != nil || !tg.CommandFrom.IsZero() || tg.CommandDisplay() != "pg_dumpall -U postgres" || tg.CommandFrom.String() != "" {
		t.Errorf("inline: target %+v, err %v", tg, err)
	}
}

func TestParseCommandErrors(t *testing.T) {
	cases := map[string]struct {
		mutate func(map[string]string)
		want   string
	}{
		"inline and configmap": {func(a map[string]string) { a[CommandConfigMap] = "cm"; a[CommandField] = "f" }, "only one of them"},
		"configmap and secret": {func(a map[string]string) {
			delete(a, Command)
			a[CommandConfigMap] = "cm"
			a[CommandSecret] = "s"
			a[CommandField] = "f"
		}, "only one of them"},
		"configmap without field": {func(a map[string]string) { delete(a, Command); a[CommandConfigMap] = "cm" },
			"missing required annotation " + CommandField + " (" + CommandConfigMap + " is set)"},
		"secret with empty field": {func(a map[string]string) { delete(a, Command); a[CommandSecret] = "s"; a[CommandField] = " " },
			"missing required annotation " + CommandField + " (" + CommandSecret + " is set)"},
		"empty configmap name": {func(a map[string]string) { delete(a, Command); a[CommandConfigMap] = " "; a[CommandField] = "f" },
			CommandConfigMap + " must not be empty"},
		"field with inline": {func(a map[string]string) { a[CommandField] = "f" }, CommandField + ": requires"},
		// A misspelt reference annotation must not go unnoticed.
		"field alone": {func(a map[string]string) { delete(a, Command); a[CommandField] = "f" }, CommandField + ": requires"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ann := valid()
			c.mutate(ann)
			_, ok, err := Parse(pod(ann, "postgres"), "@daily")
			if !ok || err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("ok=%v err=%v, want error containing %q", ok, err, c.want)
			}
		})
	}

	// None of the command annotations: the pod did not opt in.
	ann := valid()
	delete(ann, Command)
	if _, ok, err := Parse(pod(ann, "postgres"), "@daily"); ok || err != nil {
		t.Errorf("no command: ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestParsePartsCommandFrom(t *testing.T) {
	// Both commands as scripts: {PART_ID} cannot be checked yet.
	ann := partsAnn()
	delete(ann, Command)
	delete(ann, PartsCommand)
	ann[CommandSecret] = "pg-secret"
	ann[CommandField] = "dump.sh"
	ann[PartsCommandConfigMap] = "pg-scripts"
	ann[PartsCommandField] = "list.sh"
	tg, _, err := Parse(pod(ann, "postgres"), "@daily")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tg.CommandFrom != (Ref{RefSecret, "pg-secret", "dump.sh"}) || tg.PartsCommandFrom != (Ref{RefConfigMap, "pg-scripts", "list.sh"}) ||
		tg.PartsCommand != "" || tg.PartsCommandDisplay() != "configmap pg-scripts/list.sh" {
		t.Errorf("target %+v", tg)
	}

	// An inline backup command is still checked for {PART_ID}.
	ann = partsAnn()
	delete(ann, PartsCommand)
	ann[PartsCommandConfigMap] = "pg-scripts"
	ann[PartsCommandField] = "list.sh"
	ann[Command] = "pg_dumpall"
	if _, _, err := Parse(pod(ann, "postgres"), "@daily"); err == nil || !strings.Contains(err.Error(), PartPlaceholder) {
		t.Errorf("inline without placeholder: err = %v", err)
	}

	cases := map[string]func(map[string]string){
		"no parts command at all":  func(a map[string]string) { delete(a, PartsCommand) },
		"parts configmap no field": func(a map[string]string) { delete(a, PartsCommand); a[PartsCommandConfigMap] = "cm" },
		"parts inline and secret":  func(a map[string]string) { a[PartsCommandSecret] = "s"; a[PartsCommandField] = "f" },
		"parts field alone":        func(a map[string]string) { delete(a, PartsCommand); a[PartsCommandField] = "f" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ann := partsAnn()
			mutate(ann)
			if _, ok, err := Parse(pod(ann, "postgres"), "@daily"); !ok || err == nil {
				t.Fatalf("ok=%v err=%v, want true/error", ok, err)
			}
		})
	}

	// Without oap/backup-parts the parts command annotations are ignored.
	ann = valid()
	ann[PartsCommandConfigMap] = "cm"
	if tg, _, err := Parse(pod(ann, "postgres"), "@daily"); err != nil || !tg.PartsCommandFrom.IsZero() {
		t.Errorf("parts off: target %+v, err %v", tg, err)
	}
}
