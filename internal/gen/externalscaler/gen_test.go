package externalscaler_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jason-te-sde/distributed-job-queue/internal/gen/externalscaler"
)

// vendoredProtoSHA256 is the checksum of proto/externalscaler/externalscaler.proto,
// which is KEDA's file as published at the tag that proto/externalscaler/README.md
// names. The file is vendored unchanged, so a different checksum means someone
// edited it or replaced it without recording where the new one came from.
// Updating the proto means changing this constant and the README together.
const vendoredProtoSHA256 = "0e1c84a7ee1d4745eefe1fd7d16e2cbbd02763dc0cfc4c4c37d92d026deb9253"

const (
	protoPath  = "../../../proto/externalscaler/externalscaler.proto"
	readmePath = "../../../proto/externalscaler/README.md"
)

// TestVendoredProtoIsUnchanged keeps the vendored file byte-identical to the
// upstream one, and the README that records its checksum in step with it.
func TestVendoredProtoIsUnchanged(t *testing.T) {
	content, err := os.ReadFile(protoPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	if got := hex.EncodeToString(sum[:]); got != vendoredProtoSHA256 {
		t.Errorf("%s has the SHA-256 %s, want %s: the vendored file must stay as KEDA published it", protoPath, got, vendoredProtoSHA256)
	}

	readme, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), vendoredProtoSHA256) {
		t.Errorf("%s does not record the checksum %s", readmePath, vendoredProtoSHA256)
	}
}

// TestServiceDescriptor pins the RPC surface of ExternalScaler: the methods
// KEDA calls. A rename, a removal or an addition is a change of KEDA's
// protocol and must show up here.
func TestServiceDescriptor(t *testing.T) {
	svc := externalscaler.File_externalscaler_externalscaler_proto.Services().ByName("ExternalScaler")
	if svc == nil {
		t.Fatal("ExternalScaler is missing from the file descriptor")
	}
	want := []string{"IsActive", "StreamIsActive", "GetMetricSpec", "GetMetrics", "StreamMetricSpec"}
	var got []string
	for i := range svc.Methods().Len() {
		got = append(got, string(svc.Methods().Get(i).Name()))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ExternalScaler methods = %v, want %v", got, want)
	}
}
