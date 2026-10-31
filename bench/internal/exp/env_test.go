package exp

import (
	"runtime"
	"testing"
)

func TestCaptureMachineNamesTheMachine(t *testing.T) {
	env := CaptureMachine()
	if env["os"] != runtime.GOOS || env["arch"] != runtime.GOARCH {
		t.Errorf("os/arch = %q/%q, want %q/%q", env["os"], env["arch"], runtime.GOOS, runtime.GOARCH)
	}
	for _, key := range []string{"cpu_model", "cpu_count", "go_version", "gomaxprocs"} {
		if env[key] == "" {
			t.Errorf("CaptureMachine()[%q] is empty: %v", key, env)
		}
	}
	if env["go_version"] != runtime.Version() {
		t.Errorf("go_version = %q, want %q", env["go_version"], runtime.Version())
	}
}

func TestParseCPUInfo(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{"x86", "processor\t: 0\nvendor_id\t: GenuineIntel\nmodel name\t: Intel(R) Xeon(R) Platinum 8272CL CPU @ 2.60GHz\ncpu MHz\t: 2593.906\n", "Intel(R) Xeon(R) Platinum 8272CL CPU @ 2.60GHz"},
		{"arm with a model", "processor\t: 0\nModel\t\t: Raspberry Pi 4 Model B Rev 1.4\n", "Raspberry Pi 4 Model B Rev 1.4"},
		{"arm without one", "processor\t: 0\nBogoMIPS\t: 50.00\nCPU implementer\t: 0x41\n", ""},
		{"empty", "", ""},
	} {
		if got := parseCPUInfo([]byte(tc.in)); got != tc.want {
			t.Errorf("%s: parseCPUInfo = %q, want %q", tc.name, got, tc.want)
		}
	}
}
