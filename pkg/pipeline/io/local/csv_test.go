package local_test

import (
	"strings"
	"testing"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/io/local"
)

func TestReadEmailsCSV(t *testing.T) {
	t.Run("reads email column", func(t *testing.T) {
		in := "email,other\nalice@example.com,x\nbob@corp.test,y\n"
		got, err := local.ReadEmailsCSV(strings.NewReader(in))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 || got[0] != "alice@example.com" || got[1] != "bob@corp.test" {
			t.Fatalf("unexpected emails: %#v", got)
		}
	})

	t.Run("header is case-insensitive", func(t *testing.T) {
		in := "Email\nalice@example.com\n"
		got, err := local.ReadEmailsCSV(strings.NewReader(in))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0] != "alice@example.com" {
			t.Fatalf("unexpected emails: %#v", got)
		}
	})

	t.Run("missing header column errors", func(t *testing.T) {
		in := "not_email\nx\n"
		_, err := local.ReadEmailsCSV(strings.NewReader(in))
		if err == nil {
			t.Fatalf("expected error")
		}
	})
}
