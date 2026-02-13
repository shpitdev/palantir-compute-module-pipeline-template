package version

import (
	"regexp"
	"testing"
)

func TestCurrentIsSemverWithoutVPrefix(t *testing.T) {
	t.Helper()

	semver := regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	if !semver.MatchString(Current) {
		t.Fatalf("Current=%q must match <major>.<minor>.<patch>", Current)
	}
}
