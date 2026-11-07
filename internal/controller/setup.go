package controller

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/appfun-dev/imageflow/internal/clock"
	"github.com/appfun-dev/imageflow/internal/config"
)

// Options carries cross-cutting dependencies injected into every controller.
type Options struct {
	Clock  clock.Clock
	Config config.Config
}

// Registrar is implemented by each controller package so it can be registered independently.
type Registrar interface {
	Register(mgr ctrl.Manager, opts Options) error
}

// Registrars is the global list of controller registration units.
// Each controller adds itself via init() so adding a new controller only requires a new file.
var Registrars []Registrar

// Setup registers every controller in Registrars with the manager.
func Setup(mgr ctrl.Manager, opts Options) error {
	for i, r := range Registrars {
		if err := r.Register(mgr, opts); err != nil {
			return fmt.Errorf("register controller %d: %w", i, err)
		}
	}
	return nil
}
