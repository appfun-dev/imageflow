package controller

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

type fakeManager struct{ ctrl.Manager }

// fakeManagerWithMapper is a fake manager that returns a configurable
// RESTMapper. The remaining methods are the surface controller-runtime's
// builder touches when constructing a controller: scheme/cache/options/logger
// for the builder itself, client/apiReader/recorder for reconciler
// construction, and Add so tests can observe what got registered.
type fakeManagerWithMapper struct {
	fakeManager
	mapper meta.RESTMapper
	scheme *runtime.Scheme
	added  []manager.Runnable
}

func (f *fakeManagerWithMapper) GetRESTMapper() meta.RESTMapper {
	return f.mapper
}

func (f *fakeManagerWithMapper) GetScheme() *runtime.Scheme {
	if f.scheme == nil {
		return runtime.NewScheme()
	}
	return f.scheme
}

func (f *fakeManagerWithMapper) GetCache() cache.Cache { return nil }

func (f *fakeManagerWithMapper) GetControllerOptions() ctrlconfig.Controller {
	return ctrlconfig.Controller{}
}

func (f *fakeManagerWithMapper) GetLogger() logr.Logger { return logr.Discard() }

func (f *fakeManagerWithMapper) GetClient() client.Client { return nil }

func (f *fakeManagerWithMapper) GetAPIReader() client.Reader { return nil }

func (f *fakeManagerWithMapper) GetEventRecorderFor(string) record.EventRecorder { return nil }

func (f *fakeManagerWithMapper) Add(r manager.Runnable) error {
	f.added = append(f.added, r)
	return nil
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
	// T3: a passing gate must actually register the reconciler with the manager.
	if len(mgr.added) != 1 {
		t.Fatalf("expected 1 controller registered with the manager, got %d", len(mgr.added))
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
