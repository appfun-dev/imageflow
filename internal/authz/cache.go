package authz

import (
	"strings"
	"sync"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
)

// cacheKey identifies one definitive SAR verdict. It carries the full subject
// (username + groups) and the full resource attributes so verdicts can never
// leak across identities — a cached allow for one creator is never reused for
// another, which is what keeps the cache from outliving identity changes.
type cacheKey struct {
	user      string
	groups    string
	namespace string
	verb      string
	group     string
	version   string
	resource  string
	name      string
}

// groupsKeySeparator joins group lists deterministically; \x00 cannot appear
// in Kubernetes group names.
const groupsKeySeparator = "\x00"

func cacheKeyFor(subject Subject, attrs *authorizationv1.ResourceAttributes) cacheKey {
	return cacheKey{
		user:      subject.Username,
		groups:    strings.Join(subject.Groups, groupsKeySeparator),
		namespace: attrs.Namespace,
		verb:      attrs.Verb,
		group:     attrs.Group,
		version:   attrs.Version,
		resource:  attrs.Resource,
		name:      attrs.Name,
	}
}

// cacheEntry is one cached definitive verdict. checkedAt comes from the
// injected clock so TTL semantics are deterministic under tests (seam S3).
type cacheEntry struct {
	allowed   bool
	reason    string
	checkedAt time.Time
}

// resultCache is a bounded TTL cache of definitive SAR verdicts. Only
// allowed/denied results are ever inserted (unknown results must not stick).
// Size is bounded two ways: expired entries are swept on overflow, and if the
// cache is still full the oldest entries are evicted (checkedAt order), so a
// long-lived process can never grow the map without bound.
type resultCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	entries    map[cacheKey]cacheEntry
}

func newResultCache(ttl time.Duration, maxEntries int) *resultCache {
	return &resultCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[cacheKey]cacheEntry),
	}
}

// get returns the cached verdict for key when it exists and is younger than
// the TTL. Expired entries are removed on sight, so a stale allow can never
// be served after the recheck period elapsed.
func (c *resultCache) get(key cacheKey, now time.Time) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return cacheEntry{}, false
	}
	if now.Sub(entry.checkedAt) >= c.ttl {
		delete(c.entries, key)
		return cacheEntry{}, false
	}
	return entry, true
}

// put inserts or refreshes a definitive verdict stamped at now, enforcing the
// size bound (sweep expired first, then evict oldest).
func (c *resultCache) put(key cacheKey, entry cacheEntry, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.checkedAt = now
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxEntries {
		c.evictLocked(now)
	}
	c.entries[key] = entry
}

// len reports the current entry count (tests and diagnostics).
func (c *resultCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// evictLocked frees space: expired entries first, then the oldest by
// checkedAt until strictly below the bound.
func (c *resultCache) evictLocked(now time.Time) {
	for key, entry := range c.entries {
		if now.Sub(entry.checkedAt) >= c.ttl {
			delete(c.entries, key)
		}
	}
	for len(c.entries) >= c.maxEntries {
		var oldestKey cacheKey
		var oldest time.Time
		first := true
		for key, entry := range c.entries {
			if first || entry.checkedAt.Before(oldest) {
				oldestKey, oldest, first = key, entry.checkedAt, false
			}
		}
		if first {
			return
		}
		delete(c.entries, oldestKey)
	}
}
