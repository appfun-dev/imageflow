package controller

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

type fakeManager struct{ ctrl.Manager }

// fakeManagerWithMapper is a fake manager that returns a configurable RESTMapper.
type fakeManagerWithMapper struct {
	fakeManager
	mapper meta.RESTMapper
}

func (f *fakeManagerWithMapper) GetRESTMapper() meta.RESTMapper {
	return f.mapper
}

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

// BuildRun Watcher smoke tests (T1 acceptance criteria):
// - flag=false → no registration, no informer, no error
// - flag=true + CRD exists → registration succeeds
// - flag=true + CRD missing → manager startup fails with clear error

func TestBuildRunWatcher_FlagFalse_SkipsRegistration(t *testing.T) {
	reg := &buildRunWatcherRegistrar{}
	mgr := &fakeManagerWithMapper{} // mapper is nil but shouldn't be consulted

	err := reg.Register(mgr, Options{
		Config: config.Config{EnableBuildRunWatcher: false},
	})
	if err != nil {
		t.Fatalf("expected nil error when flag=false, got: %v", err)
	}
}

func TestBuildRunWatcher_FlagTrue_CRDExists_RegistersSuccessfully(t *testing.T) {
	// Build a RESTMapper that knows about shipwright.io/v1beta1 Build.
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Group: shipwrightBuildCRDGroup, Version: shipwrightBuildCRDVersion},
	})
	mapper.Add(shipwrightBuildGVK, meta.RESTScopeNamespace)

	reg := &buildRunWatcherRegistrar{}
	mgr := &fakeManagerWithMapper{mapper: mapper}

	err := reg.Register(mgr, Options{
		Config: config.Config{EnableBuildRunWatcher: true},
	})
	if err != nil {
		t.Fatalf("expected nil error when flag=true and CRD exists, got: %v", err)
	}
}

func TestBuildRunWatcher_FlagTrue_CRDMissing_ReturnsError(t *testing.T) {
	// Empty RESTMapper: no Shipwright CRDs registered.
	mapper := meta.NewDefaultRESTMapper(nil)

	reg := &buildRunWatcherRegistrar{}
	mgr := &fakeManagerWithMapper{mapper: mapper}

	err := reg.Register(mgr, Options{
		Config: config.Config{EnableBuildRunWatcher: true},
	})
	if err == nil {
		t.Fatal("expected error when flag=true and CRD missing, got nil")
	}
	if !strings.Contains(err.Error(), "shipwright.io") {
		t.Errorf("error should mention shipwright.io, got: %v", err)
	}
	if !strings.Contains(err.Error(), "--enable-buildrun-watcher") {
		t.Errorf("error should mention the flag name, got: %v", err)
	}
}
