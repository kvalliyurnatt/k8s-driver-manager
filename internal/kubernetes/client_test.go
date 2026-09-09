/*
 * Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kubernetes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// flakyNodeAPI is a minimal stand-in for the Kubernetes API server that serves a
// single Node and fails the first getFailures reads and patchFailures writes
// with a 500, mimicking an API server that is not yet ready when the driver
// manager starts.
type flakyNodeAPI struct {
	mu            sync.Mutex
	nodeName      string
	getFailures   int
	patchFailures int
	requests      int
	patches       int
	unschedulable bool
}

func (f *flakyNodeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests++

	// The cordon path is a read followed by a write, and either half can fail
	// independently, so the failure budgets are tracked per verb.
	write := r.Method == http.MethodPatch || r.Method == http.MethodPut
	if write {
		f.patches++
		if f.patchFailures > 0 {
			f.patchFailures--
			http.Error(w, "the server rejected the update", http.StatusInternalServerError)
			return
		}
	} else if f.getFailures > 0 {
		f.getFailures--
		http.Error(w, "the server is currently unable to handle the request", http.StatusInternalServerError)
		return
	}

	if write {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// A strategic merge patch clears spec.unschedulable with a null rather
		// than setting it to false, so key presence is what matters here.
		var patch map[string]any
		if err := json.Unmarshal(body, &patch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if spec, ok := patch["spec"].(map[string]any); ok {
			if value, present := spec["unschedulable"]; present {
				cordoned, _ := value.(bool)
				f.unschedulable = cordoned
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	node := &corev1.Node{
		TypeMeta:   metav1.TypeMeta{Kind: "Node", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: f.nodeName},
		Spec:       corev1.NodeSpec{Unschedulable: f.unschedulable},
	}
	if err := json.NewEncoder(w).Encode(node); err != nil {
		f.requests-- // the request never completed, do not count it
	}
}

func (f *flakyNodeAPI) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func (f *flakyNodeAPI) cordoned() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unschedulable
}

func discardLogger() *logrus.Logger {
	log := logrus.New()
	log.SetOutput(io.Discard)
	return log
}

func newTestClient(t *testing.T, api *flakyNodeAPI) *Client {
	t.Helper()

	server := httptest.NewServer(api)
	t.Cleanup(server.Close)

	clientset, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)

	return &Client{ctx: context.Background(), log: discardLogger(), clientset: clientset}
}

func (f *flakyNodeAPI) patchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.patches
}

// The client makes one attempt per call: the retry policy lives in the
// driver-manager command, not here.
func TestCordonNodeCordonsTheNode(t *testing.T) {
	api := &flakyNodeAPI{nodeName: "gpu-node"}
	c := newTestClient(t, api)

	require.NoError(t, c.CordonNode("gpu-node"))
	require.True(t, api.cordoned())
	// One read followed by one write.
	require.Equal(t, 1, api.patchCount())
	require.Equal(t, 2, api.requestCount())
}

func TestUncordonNodeUncordonsTheNode(t *testing.T) {
	api := &flakyNodeAPI{nodeName: "gpu-node", unschedulable: true}
	c := newTestClient(t, api)

	require.NoError(t, c.UncordonNode("gpu-node"))
	require.False(t, api.cordoned())
	require.Equal(t, 1, api.patchCount())
	require.Equal(t, 2, api.requestCount())
}

func TestCordonNodeReturnsErrorWhenGetFails(t *testing.T) {
	api := &flakyNodeAPI{nodeName: "gpu-node", getFailures: 1}
	c := newTestClient(t, api)

	require.Error(t, c.CordonNode("gpu-node"))
	require.False(t, api.cordoned())
	require.Equal(t, 1, api.requestCount())
}

func TestCordonNodeReturnsErrorWhenPatchFails(t *testing.T) {
	api := &flakyNodeAPI{nodeName: "gpu-node", patchFailures: 1}
	c := newTestClient(t, api)

	require.Error(t, c.CordonNode("gpu-node"))
	require.False(t, api.cordoned())
	require.Equal(t, 1, api.patchCount())
}

func TestUncordonNodeReturnsErrorWhenGetFails(t *testing.T) {
	api := &flakyNodeAPI{nodeName: "gpu-node", getFailures: 1, unschedulable: true}
	c := newTestClient(t, api)

	require.Error(t, c.UncordonNode("gpu-node"))
	require.True(t, api.cordoned())
	require.Equal(t, 1, api.requestCount())
}

func TestUncordonNodeReturnsErrorWhenPatchFails(t *testing.T) {
	api := &flakyNodeAPI{nodeName: "gpu-node", patchFailures: 1, unschedulable: true}
	c := newTestClient(t, api)

	require.Error(t, c.UncordonNode("gpu-node"))
	require.True(t, api.cordoned())
	require.Equal(t, 1, api.patchCount())
}
