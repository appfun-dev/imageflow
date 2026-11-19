package config

import (
	"flag"
	"testing"
	"time"
)

func TestFlagsParseWithDefaults(t *testing.T) {
	var cfg Config
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.BindFlags(fs)

	if err := fs.Parse([]string{
		"--mapping-ttl-success=2h",
		"--mapping-ttl-failure=48h",
		"--import-default-interval=30s",
		"--scheduled-default-interval=10m",
		"--sar-recheck-period=1m",
		"--stream-tag-history-limit=7",
		"--subscription-delivery-limit=10",
		"--mirror-event-limit=8",
		"--registry-allowlist=registry.example.com,registry.internal.io",
	}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	if cfg.MappingTTLSuccess != 2*time.Hour {
		t.Errorf("mapping-ttl-success = %v", cfg.MappingTTLSuccess)
	}
	if cfg.MappingTTLFailure != 48*time.Hour {
		t.Errorf("mapping-ttl-failure = %v", cfg.MappingTTLFailure)
	}
	if cfg.ImportDefaultInterval != 30*time.Second {
		t.Errorf("import-default-interval = %v", cfg.ImportDefaultInterval)
	}
	if cfg.ScheduledDefaultInterval != 10*time.Minute {
		t.Errorf("scheduled-default-interval = %v", cfg.ScheduledDefaultInterval)
	}
	if cfg.SARRecheckPeriod != time.Minute {
		t.Errorf("sar-recheck-period = %v", cfg.SARRecheckPeriod)
	}
	if cfg.StreamTagHistoryLimit != 7 {
		t.Errorf("stream-tag-history-limit = %d", cfg.StreamTagHistoryLimit)
	}
	if cfg.SubscriptionDeliveryLimit != 10 {
		t.Errorf("subscription-delivery-limit = %d", cfg.SubscriptionDeliveryLimit)
	}
	if cfg.MirrorEventLimit != 8 {
		t.Errorf("mirror-event-limit = %d", cfg.MirrorEventLimit)
	}
	if len(cfg.RegistryAllowlist) != 2 {
		t.Errorf("registry-allowlist = %v", cfg.RegistryAllowlist)
	}
}

func TestDefaultsMatchADR008(t *testing.T) {
	var cfg Config
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.BindFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if cfg.StreamTagHistoryLimit != 3 || cfg.SubscriptionDeliveryLimit != 5 || cfg.MirrorEventLimit != 5 {
		t.Errorf("status bounds defaults do not match 3/5/5: %d/%d/%d",
			cfg.StreamTagHistoryLimit, cfg.SubscriptionDeliveryLimit, cfg.MirrorEventLimit)
	}
}

// v1.7-06 (#83): the hard bound on how many objects one label-selector
// target may match per delivery pass — flag default 50 (ADR-014 decision 7),
// configurable at startup, non-positive rejected.
func TestSelectorMatchLimitFlag(t *testing.T) {
	if DefaultSelectorMatchLimit != 50 {
		t.Errorf("DefaultSelectorMatchLimit = %d, want 50 (ADR-014 decision 7)", DefaultSelectorMatchLimit)
	}

	var cfg Config
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.BindFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if cfg.SelectorMatchLimit != DefaultSelectorMatchLimit {
		t.Errorf("--selector-match-limit default = %d, want %d",
			cfg.SelectorMatchLimit, DefaultSelectorMatchLimit)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate failed with flag defaults: %v", err)
	}

	var custom Config
	customFS := flag.NewFlagSet("test", flag.ContinueOnError)
	custom.BindFlags(customFS)
	if err := customFS.Parse([]string{"--selector-match-limit=7"}); err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if custom.SelectorMatchLimit != 7 {
		t.Errorf("--selector-match-limit = %d, want 7", custom.SelectorMatchLimit)
	}
	if err := custom.Validate(); err != nil {
		t.Fatalf("validate rejected a configured positive limit: %v", err)
	}

	for _, value := range []string{"0", "-1"} {
		var bad Config
		badFS := flag.NewFlagSet("test", flag.ContinueOnError)
		bad.BindFlags(badFS)
		if err := badFS.Parse([]string{"--selector-match-limit=" + value}); err != nil {
			t.Fatalf("parse %s failed: %v", value, err)
		}
		if err := bad.Validate(); err == nil {
			t.Errorf("Validate() accepted --selector-match-limit=%s, want an error", value)
		}
	}
}
