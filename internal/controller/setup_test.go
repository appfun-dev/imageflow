package controller

import (
	"errors"
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

type fakeManager struct{ ctrl.Manager }

type fakeRegistrar struct {
	called bool
	err    error
}

func (f *fakeRegistrar) Register(mgr ctrl.Manager, opts Options) error {
	f.called = true
	return f.err
}

func TestSetupCallsAllRegistrars(t *testing.T) {
	r1 := &fakeRegistrar{}
	r2 := &fakeRegistrar{}

	// preserve and restore global state
	old := Registrars
	defer func() { Registrars = old }()
	Registrars = []Registrar{r1, r2}

	if err := Setup(&fakeManager{}, Options{Clock: clock.Real{}, Config: config.Config{}}); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	if !r1.called || !r2.called {
		t.Fatalf("not all registrars were called: r1=%v r2=%v", r1.called, r2.called)
	}
}

func TestSetupReturnsFirstError(t *testing.T) {
	want := errors.New("boom")
	r1 := &fakeRegistrar{err: want}

	old := Registrars
	defer func() { Registrars = old }()
	Registrars = []Registrar{r1}

	if err := Setup(&fakeManager{}, Options{}); err == nil || !errors.Is(err, want) {
		t.Fatalf("expected error %v, got %v", want, err)
	}
}
