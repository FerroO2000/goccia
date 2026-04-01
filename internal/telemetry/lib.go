package telemetry

import "runtime/debug"

const libName = "github.com/FerroO2000/goccia"

var libVersion = getLibVersion()

func getLibVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		// When used as a dependency
		for _, dep := range info.Deps {
			if dep.Path == libName {
				return dep.Version
			}
		}

		// When running as the main module (tests, examples)
		if info.Main.Path == libName {
			return info.Main.Version
		}
	}

	return "dev"
}
