// Package e2e runs the job queue on a Kubernetes cluster: the image, the Helm
// chart, the example worker and the KEDA scaler, together, under the faults
// that a cluster brings (a pod that is deleted, a rolling restart) and under
// the autoscaler's own scaling.
//
// The tests need a kind cluster with KEDA installed in the namespace keda, a
// kubectl whose current context is that cluster, helm, and the image already
// loaded into the cluster. They do not create any of these. CI sets them up
// in the k8s-e2e job of .github/workflows/ci.yml; a local run is
//
//	kind create cluster --name jq-e2e --config deploy/kind/kind-config.yaml
//	helm repo add kedacore https://kedacore.github.io/charts
//	helm install keda kedacore/keda --version 2.21.0 --namespace keda --create-namespace --wait
//	docker build -t jq:e2e . && kind load docker-image jq:e2e --name jq-e2e
//	JOBQUEUE_E2E_IMAGE=jq:e2e go test -tags e2e -count=1 -timeout 40m ./test/e2e/
//
// A missing piece fails the run: nothing here skips. The run leaves its
// namespace behind, so that the objects can be inspected after a failure, and
// refuses to start where the namespace already exists.
//
// The run is pinned to one cluster. It starts only when the current kubectl
// context is a kind context (kind-...) whose API server is on a loopback
// address, and then it names that context in every kubectl and helm command,
// so that a later change of the current context, by kind or by anything else
// on the machine, cannot send an install, a pod deletion or a restart to
// another cluster. The untagged tests of this package check that rule.
package e2e
