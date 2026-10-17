package config

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

// Config holds all controller-manager flags so they can be injected into controllers.
type Config struct {
	MappingTTLSuccess         time.Duration
	MappingTTLFailure         time.Duration
	ImportDefaultInterval     time.Duration
	ScheduledDefaultInterval  time.Duration
	SARRecheckPeriod          time.Duration
	StreamTagHistoryLimit     int
	SubscriptionDeliveryLimit int
	MirrorEventLimit          int
	RegistryAllowlist         []string
}

// BindFlags registers all ImageFlow controller flags on the supplied flag set.
func (c *Config) BindFlags(fs *flag.FlagSet) {
	fs.DurationVar(&c.MappingTTLSuccess, "mapping-ttl-success", time.Hour,
		"TTL after which a successful ImageStreamMapping is garbage collected.")
	fs.DurationVar(&c.MappingTTLFailure, "mapping-ttl-failure", 24*time.Hour,
		"TTL after which a failed ImageStreamMapping is garbage collected.")
	fs.DurationVar(&c.ImportDefaultInterval, "import-default-interval", 15*time.Minute,
		"Default interval for on-demand ImageStream import polling.")
	fs.DurationVar(&c.ScheduledDefaultInterval, "scheduled-default-interval", 15*time.Minute,
		"Default interval for scheduled ImageStream tag imports.")
	fs.DurationVar(&c.SARRecheckPeriod, "sar-recheck-period", 10*time.Minute,
		"How often to re-check SubjectAccessReview permissions for subscription targets "+
			"(ADR-003 default; also the SAR result cache TTL of the T8 authorization gate).")
	fs.IntVar(&c.StreamTagHistoryLimit, "stream-tag-history-limit", 3,
		"Maximum number of historic images kept per ImageStream tag.")
	fs.IntVar(&c.SubscriptionDeliveryLimit, "subscription-delivery-limit", 5,
		"Maximum number of recent delivery results kept in an ImageSubscription status.")
	fs.IntVar(&c.MirrorEventLimit, "mirror-event-limit", 5,
		"Maximum number of recent events kept in mirror (ImageStreamTag/ImageTag) statuses.")
	fs.Var(&stringSliceValue{dst: &c.RegistryAllowlist}, "registry-allowlist",
		"Comma-separated list of allowed registry hostnames (empty = allow all).")
}

// Validate performs lightweight cross-field checks after flags are parsed.
func (c *Config) Validate() error {
	if c.MappingTTLSuccess <= 0 {
		return fmt.Errorf("--mapping-ttl-success must be positive")
	}
	if c.MappingTTLFailure <= 0 {
		return fmt.Errorf("--mapping-ttl-failure must be positive")
	}
	if c.StreamTagHistoryLimit <= 0 || c.SubscriptionDeliveryLimit <= 0 || c.MirrorEventLimit <= 0 {
		return fmt.Errorf("status bounds must be positive")
	}
	return nil
}

type stringSliceValue struct {
	dst *[]string
}

func (v *stringSliceValue) String() string {
	if v.dst == nil {
		return ""
	}
	return strings.Join(*v.dst, ",")
}

func (v *stringSliceValue) Set(value string) error {
	parts := strings.Split(value, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			*v.dst = append(*v.dst, p)
		}
	}
	return nil
}
