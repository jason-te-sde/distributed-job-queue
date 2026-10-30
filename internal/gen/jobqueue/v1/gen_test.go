package jobqueuev1_test

import (
	"slices"
	"testing"

	jobqueuev1 "github.com/jason-te-sde/distributed-job-queue/internal/gen/jobqueue/v1"
)

// TestServiceDescriptor pins the RPC surface of JobQueueService. Renaming,
// dropping or adding a method is a protocol change and must show up here.
func TestServiceDescriptor(t *testing.T) {
	svc := jobqueuev1.File_jobqueue_v1_jobqueue_proto.Services().ByName("JobQueueService")
	if svc == nil {
		t.Fatal("JobQueueService is missing from the file descriptor")
	}

	want := []string{
		"Enqueue", "Lease", "Heartbeat", "Ack", "Release", "GetJob",
		"CancelJob", "GetStats", "ListDeadJobs", "RequeueJob", "PurgeJobs",
	}
	var got []string
	for i := range svc.Methods().Len() {
		got = append(got, string(svc.Methods().Get(i).Name()))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("JobQueueService methods = %v, want %v", got, want)
	}
}
