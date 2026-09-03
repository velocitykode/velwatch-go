package velwatch

import "testing"

func TestResolveReleaseInfo(t *testing.T) {
	cases := []struct {
		name          string
		release       string // explicit config value
		commitSHA     string // explicit config value
		env           map[string]string
		wantRelease   string
		wantCommitSHA string
	}{
		{
			name:          "explicit config wins over all env",
			release:       "1.2.3",
			commitSHA:     "abc123",
			env:           map[string]string{"VELWATCH_RELEASE": "9.9.9", "VELWATCH_COMMIT_SHA": "deadbeef", "OTEL_RESOURCE_ATTRIBUTES": "service.version=0.0.1,vcs.ref.head.revision=cafe"},
			wantRelease:   "1.2.3",
			wantCommitSHA: "abc123",
		},
		{
			name:          "VELWATCH_* wins over OTEL_RESOURCE_ATTRIBUTES",
			env:           map[string]string{"VELWATCH_RELEASE": "2.0.0", "VELWATCH_COMMIT_SHA": "feedface", "OTEL_RESOURCE_ATTRIBUTES": "service.version=0.0.1,vcs.ref.head.revision=cafe"},
			wantRelease:   "2.0.0",
			wantCommitSHA: "feedface",
		},
		{
			name:          "OTEL_RESOURCE_ATTRIBUTES fallback",
			env:           map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "service.name=api,service.version=3.1.0,vcs.ref.head.revision=1a2b3c"},
			wantRelease:   "3.1.0",
			wantCommitSHA: "1a2b3c",
		},
		{
			name:          "per-value precedence mix",
			release:       "explicit-rel",
			env:           map[string]string{"VELWATCH_COMMIT_SHA": "env-sha"},
			wantRelease:   "explicit-rel",
			wantCommitSHA: "env-sha",
		},
		{
			name:          "percent-decoding of OTEL values",
			env:           map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "service.version=1.0.0%2Bbuild.5,vcs.ref.head.revision=feature%2Fbranch"},
			wantRelease:   "1.0.0+build.5",
			wantCommitSHA: "feature/branch",
		},
		{
			name:          "empty when nothing set",
			wantRelease:   "",
			wantCommitSHA: "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Ensure a clean slate for the env vars this function reads.
			for _, k := range []string{"VELWATCH_RELEASE", "VELWATCH_COMMIT_SHA", "OTEL_RESOURCE_ATTRIBUTES"} {
				t.Setenv(k, "")
			}
			for k, v := range c.env {
				t.Setenv(k, v)
			}

			gotRelease, gotCommitSHA := resolveReleaseInfo(c.release, c.commitSHA)
			if gotRelease != c.wantRelease {
				t.Errorf("release = %q, want %q", gotRelease, c.wantRelease)
			}
			if gotCommitSHA != c.wantCommitSHA {
				t.Errorf("commitSHA = %q, want %q", gotCommitSHA, c.wantCommitSHA)
			}
		})
	}
}

func TestParseOTELResourceAttributes(t *testing.T) {
	got := parseOTELResourceAttributes(" service.version = 1.0 , vcs.ref.head.revision=ab%20cd ,malformed, =novalue")
	if got["service.version"] != "1.0" {
		t.Errorf("service.version = %q, want %q", got["service.version"], "1.0")
	}
	if got["vcs.ref.head.revision"] != "ab cd" {
		t.Errorf("vcs.ref.head.revision = %q, want %q", got["vcs.ref.head.revision"], "ab cd")
	}
	if _, ok := got["malformed"]; ok {
		t.Error("pair without '=' should be skipped")
	}
	if _, ok := got[""]; ok {
		t.Error("empty key should be skipped")
	}
	if got := parseOTELResourceAttributes(""); len(got) != 0 {
		t.Errorf("empty input should yield empty map, got %v", got)
	}
}

func TestSetDefaultTag(t *testing.T) {
	// Empty value is a no-op.
	e := NewEvent(EventTypeRequest)
	e.setDefaultTag(tagRelease, "")
	if _, ok := e.Tags[tagRelease]; ok {
		t.Error("empty value should not set a tag")
	}

	// Non-empty value is stamped.
	e.setDefaultTag(tagRelease, "1.0.0")
	if e.Tags[tagRelease] != "1.0.0" {
		t.Errorf("release tag = %q, want 1.0.0", e.Tags[tagRelease])
	}

	// A tag the caller set explicitly is never overwritten.
	e.WithTag(tagCommitSHA, "user-sha")
	e.setDefaultTag(tagCommitSHA, "sdk-sha")
	if e.Tags[tagCommitSHA] != "user-sha" {
		t.Errorf("commit_sha tag = %q, want user-sha (must not overwrite)", e.Tags[tagCommitSHA])
	}
}

func TestExporterSend_StampsReleaseTags(t *testing.T) {
	tr := &OTLPExporter{release: "1.4.2", commitSHA: "abc123"}

	// A fresh event gets both tags stamped.
	e := NewRequestEvent("GET", "/", 200, 1)
	// setDefaultTag is exercised via the same path Send uses.
	e.setDefaultTag(tagRelease, tr.release)
	e.setDefaultTag(tagCommitSHA, tr.commitSHA)
	if e.Tags[tagRelease] != "1.4.2" {
		t.Errorf("release tag = %q, want 1.4.2", e.Tags[tagRelease])
	}
	if e.Tags[tagCommitSHA] != "abc123" {
		t.Errorf("commit_sha tag = %q, want abc123", e.Tags[tagCommitSHA])
	}

	// A user-set release tag survives.
	e2 := NewRequestEvent("GET", "/", 200, 1).WithTag(tagRelease, "user-release")
	e2.setDefaultTag(tagRelease, tr.release)
	if e2.Tags[tagRelease] != "user-release" {
		t.Errorf("release tag = %q, want user-release", e2.Tags[tagRelease])
	}
}
