// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
)

// TestReproResumeCanceledAfterPush reproduces google/ax#452 on main
// (775f5ad): the runner accepts the push (202), the actor resumes to RUNNING,
// and then the RPC context is canceled. ax reports "runner ... did not accept
// the task" even though it did, persists nothing, and the task is left
// Ready=False until the next successful write.
//
// This is a reproduction, not a regression test: it asserts the current
// (buggy) behavior. Once the reconcile and the status write are detached from
// the caller's context, the assertions should be inverted.
func TestReproResumeCanceledAfterPush(t *testing.T) {
	pushedCh := make(chan struct{}, 1)
	readyPolledCh := make(chan struct{}, 1)
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == v1alpha1.RunnerTaskPath {
			// Accept first: from here on the runner holds the task, so it
			// runs whether or not the caller is still waiting.
			w.WriteHeader(http.StatusAccepted)
			select {
			case pushedCh <- struct{}{}:
			default:
			}
			return
		}
		// The workspace never becomes ready while the resume is open.
		http.Error(w, "", http.StatusServiceUnavailable)
		select {
		case readyPolledCh <- struct{}{}:
		default:
		}
	}))
	defer runner.Close()
	t.Setenv("ATENET_ROUTER_ADDR", runner.URL)

	st := memory.NewStore()
	sub := substratetest.NewServer(t)
	srv := server.NewServer(st, newTestReconciler(sub))
	base := context.Background()

	if _, err := srv.CreateTask(base, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "cancel-repro"},
		Spec:     &v1alpha1.TaskSpec{Profile: &v1alpha1.Profile{Image: "alpine"}},
	}}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	ctx, cancel := context.WithCancel(base)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "cancel-repro"})
		errCh <- err
	}()

	select {
	case <-pushedCh:
	case <-time.After(15 * time.Second):
		t.Fatal("runner never received the push")
	}
	// Wait for the readiness poll too, so the push has been accepted and the
	// resume is in its wait phase when the caller cancels.
	select {
	case <-readyPolledCh:
	case <-time.After(15 * time.Second):
		t.Fatal("resume never polled the workspace readiness")
	}
	cancel()

	var resumeErr error
	select {
	case resumeErr = <-errCh:
	case <-time.After(15 * time.Second):
		t.Fatal("ResumeTask did not return after cancellation")
	}
	t.Logf("ResumeTask error after cancel: %v", resumeErr)

	actor, ok := sub.Actor(v1alpha1.DefaultAtespace, "cancel-repro")
	if !ok {
		t.Fatal("no actor was created")
	}
	t.Logf("actor state on Substrate: %v", actor.GetStatus().GetState())

	stored, err := st.GetTask(base, v1alpha1.DefaultAtespace, "cancel-repro")
	if err != nil {
		t.Fatalf("stored GetTask: %v", err)
	}
	t.Logf("stored status: state=%v conditions=%v", stored.GetStatus().GetState(), stored.GetStatus().GetConditions())

	got, err := srv.GetTask(base, &v1alpha1.GetTaskRequest{Atespace: v1alpha1.DefaultAtespace, Name: "cancel-repro"})
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	var ready *v1alpha1.Condition
	for _, c := range got.GetStatus().GetConditions() {
		if c.GetType() == v1alpha1.ConditionReady {
			ready = c
		}
	}
	t.Logf("observed status: state=%v ready=%v reason=%v message=%q",
		got.GetStatus().GetState(), ready.GetStatus(), ready.GetReason(), ready.GetMessage())

	// The reproduction, asserted: the push was accepted (the task runs) and
	// the actor is RUNNING, yet the RPC fails and no status is persisted.
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("actor state = %v, want RUNNING (the push was accepted, so the task runs)", actor.GetStatus().GetState())
	}
	if resumeErr == nil {
		t.Fatal("ResumeTask returned nil; want the status-write failure caused by the canceled context")
	}
	if ready.GetStatus() != "False" || !strings.Contains(ready.GetMessage(), "Waiting for workspace setup") {
		t.Fatalf("observed Ready = %v/%q, want False with a stale workspace-setup message on a running task",
			ready.GetStatus(), ready.GetMessage())
	}
}
