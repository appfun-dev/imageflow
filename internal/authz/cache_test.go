package authz

import (
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
)

// Direct cache unit tests complement the gate-level TTL/bound specs: exact
// expiry boundary, sweep-before-evict behavior and refresh semantics.

func attrsFor(name string) *authorizationv1.ResourceAttributes {
	return &authorizationv1.ResourceAttributes{
		Namespace: testNamespace,
		Verb:      sarVerb,
		Group:     "apps",
		Version:   "v1",
		Resource:  "deployments",
		Name:      name,
	}
}

func TestCacheExpiryBoundaryIsInclusive(t *testing.T) {
	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	c := newResultCache(5*time.Minute, 16)
	key := cacheKeyFor(userSubject(), attrsFor("boundary-app"))
	c.put(key, cacheEntry{allowed: true}, now)

	if _, ok := c.get(key, now.Add(5*time.Minute-time.Nanosecond)); !ok {
		t.Fatal("entry must be served just before the TTL boundary")
	}
	if _, ok := c.get(key, now.Add(5*time.Minute)); ok {
		t.Fatal("entry must expire exactly at the TTL boundary (a cached verdict never outlives one recheck period)")
	}
	if c.len() != 0 {
		t.Errorf("expired entry must be removed on sight, cache size = %d", c.len())
	}
}

func TestCachePutRefreshesTimestamp(t *testing.T) {
	now := time.Date(2026, 4, 2, 8, 30, 0, 0, time.UTC)
	c := newResultCache(time.Hour, 16)
	key := cacheKeyFor(userSubject(), attrsFor("refresh-app"))
	c.put(key, cacheEntry{allowed: false, reason: "first"}, now)
	c.put(key, cacheEntry{allowed: true}, now.Add(30*time.Minute))

	entry, ok := c.get(key, now.Add(80*time.Minute))
	if !ok {
		t.Fatal("refreshed entry must still be live 50min after the refresh (TTL 1h)")
	}
	if !entry.allowed {
		t.Error("the refreshed verdict must replace the old one")
	}
	if c.len() != 1 {
		t.Errorf("cache size = %d, want 1", c.len())
	}
}

func TestCacheEvictionSweepsExpiredBeforeOldest(t *testing.T) {
	now := time.Date(2026, 4, 3, 0, 0, 0, 0, time.UTC)
	c := newResultCache(time.Minute, 2)
	c.put(cacheKeyFor(userSubject(), attrsFor("old-but-live")), cacheEntry{allowed: true}, now)
	c.put(cacheKeyFor(userSubject(), attrsFor("doomed")), cacheEntry{allowed: true}, now)

	// 2 minutes later "doomed" era entries are expired; a fresh insert must
	// sweep them instead of evicting the just-inserted live entry.
	later := now.Add(2 * time.Minute)
	c.put(cacheKeyFor(userSubject(), attrsFor("fresh")), cacheEntry{allowed: true}, later)

	if c.len() != 1 {
		t.Fatalf("cache size = %d, want 1 after sweeping expired entries", c.len())
	}
	if _, ok := c.get(cacheKeyFor(userSubject(), attrsFor("fresh")), later); !ok {
		t.Error("the fresh entry must survive the eviction sweep")
	}
}

func TestCacheEvictsOldestWhenAllLive(t *testing.T) {
	now := time.Date(2026, 4, 4, 6, 0, 0, 0, time.UTC)
	c := newResultCache(time.Hour, 2)
	c.put(cacheKeyFor(userSubject(), attrsFor("oldest")), cacheEntry{allowed: true}, now)
	c.put(cacheKeyFor(userSubject(), attrsFor("middle")), cacheEntry{allowed: true}, now.Add(time.Minute))
	c.put(cacheKeyFor(userSubject(), attrsFor("newest")), cacheEntry{allowed: true}, now.Add(2*time.Minute))

	if c.len() != 2 {
		t.Fatalf("cache size = %d, want the bound of 2", c.len())
	}
	at := now.Add(3 * time.Minute)
	if _, ok := c.get(cacheKeyFor(userSubject(), attrsFor("oldest")), at); ok {
		t.Error("the oldest live entry must be evicted on overflow")
	}
	if _, ok := c.get(cacheKeyFor(userSubject(), attrsFor("newest")), at); !ok {
		t.Error("the newest entry must be retained")
	}
}
