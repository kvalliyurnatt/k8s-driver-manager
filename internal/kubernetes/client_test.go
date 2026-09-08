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
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const testNodeName = "test-node"

func newTestClient(node *corev1.Node) (*Client, *fake.Clientset) {
	clientset := fake.NewSimpleClientset(node)
	return &Client{
		ctx:       context.Background(),
		log:       logrus.New(),
		clientset: clientset,
	}, clientset
}

func newTestNode(unschedulable bool, annotations map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testNodeName,
			ResourceVersion: "1",
			Annotations:     annotations,
		},
		Spec: corev1.NodeSpec{Unschedulable: unschedulable},
	}
}

func getTestNode(t *testing.T, clientset *fake.Clientset) *corev1.Node {
	t.Helper()
	node, err := clientset.CoreV1().Nodes().Get(t.Context(), testNodeName, metav1.GetOptions{})
	require.NoError(t, err)
	return node
}

func getNodePatches(t *testing.T, clientset *fake.Clientset) []map[string]interface{} {
	t.Helper()

	var patches []map[string]interface{}
	for _, action := range clientset.Actions() {
		patchAction, ok := action.(k8stesting.PatchAction)
		if !ok || action.GetResource().Resource != "nodes" {
			continue
		}

		var patch map[string]interface{}
		require.NoError(t, json.Unmarshal(patchAction.GetPatch(), &patch))
		patches = append(patches, patch)
	}
	return patches
}

func TestCordonUncordonNode(t *testing.T) {
	testCases := []struct {
		name                  string
		unschedulable         bool
		annotations           map[string]string
		cordon                bool
		expectedUnschedulable bool
	}{
		{
			name:                  "schedulable node is restored",
			cordon:                true,
			expectedUnschedulable: false,
		},
		{
			name:                  "pre-existing cordon is preserved",
			unschedulable:         true,
			cordon:                true,
			expectedUnschedulable: true,
		},
		{
			name:                  "recording survives a restart",
			unschedulable:         true,
			annotations:           map[string]string{nodeInitialUnschedulableAnnotationKey: "false"},
			cordon:                true,
			expectedUnschedulable: false,
		},
		{
			name:                  "stale recording on schedulable node is replaced",
			annotations:           map[string]string{nodeInitialUnschedulableAnnotationKey: "true"},
			cordon:                true,
			expectedUnschedulable: false,
		},
		{
			name:                  "external cordon without recording is preserved",
			unschedulable:         true,
			expectedUnschedulable: true,
		},
		{
			name:                  "schedulable node without recording is unchanged",
			expectedUnschedulable: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client, clientset := newTestClient(newTestNode(tc.unschedulable, tc.annotations))
			if tc.cordon {
				require.NoError(t, client.CordonNode(testNodeName))
				require.True(t, getTestNode(t, clientset).Spec.Unschedulable)
			}

			require.NoError(t, client.UncordonNode(testNodeName))
			node := getTestNode(t, clientset)
			require.Equal(t, tc.expectedUnschedulable, node.Spec.Unschedulable)
			require.NotContains(t, node.Annotations, nodeInitialUnschedulableAnnotationKey)
		})
	}
}

func TestCordonNodeRecordsInitialState(t *testing.T) {
	testCases := []struct {
		name               string
		unschedulable      bool
		annotations        map[string]string
		expectedAnnotation string
		expectedPatches    int
	}{
		{
			name:               "schedulable",
			expectedAnnotation: "false",
			expectedPatches:    1,
		},
		{
			name:               "already cordoned",
			unschedulable:      true,
			expectedAnnotation: "true",
			expectedPatches:    1,
		},
		{
			name:               "existing recording is retained while cordoned",
			unschedulable:      true,
			annotations:        map[string]string{nodeInitialUnschedulableAnnotationKey: "false"},
			expectedAnnotation: "false",
		},
		{
			name:               "stale recording is replaced while schedulable",
			annotations:        map[string]string{nodeInitialUnschedulableAnnotationKey: "true"},
			expectedAnnotation: "false",
			expectedPatches:    1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client, clientset := newTestClient(newTestNode(tc.unschedulable, tc.annotations))
			require.NoError(t, client.CordonNode(testNodeName))

			node := getTestNode(t, clientset)
			require.True(t, node.Spec.Unschedulable)
			require.Equal(t, tc.expectedAnnotation, node.Annotations[nodeInitialUnschedulableAnnotationKey])
			require.Len(t, getNodePatches(t, clientset), tc.expectedPatches)
		})
	}
}

func TestNodeSchedulingStatePatchesAreAtomic(t *testing.T) {
	t.Run("cordon", func(t *testing.T) {
		client, clientset := newTestClient(newTestNode(false, nil))
		require.NoError(t, client.CordonNode(testNodeName))

		patches := getNodePatches(t, clientset)
		require.Len(t, patches, 1)
		require.Equal(t, true, patches[0]["spec"].(map[string]interface{})["unschedulable"])
		metadata := patches[0]["metadata"].(map[string]interface{})
		require.Equal(t, "1", metadata["resourceVersion"])
		require.Equal(t, "false", metadata["annotations"].(map[string]interface{})[nodeInitialUnschedulableAnnotationKey])
	})

	t.Run("uncordon", func(t *testing.T) {
		client, clientset := newTestClient(newTestNode(
			true,
			map[string]string{nodeInitialUnschedulableAnnotationKey: "false"},
		))
		require.NoError(t, client.UncordonNode(testNodeName))

		patches := getNodePatches(t, clientset)
		require.Len(t, patches, 1)
		require.Equal(t, false, patches[0]["spec"].(map[string]interface{})["unschedulable"])
		metadata := patches[0]["metadata"].(map[string]interface{})
		require.Equal(t, "1", metadata["resourceVersion"])
		require.Nil(t, metadata["annotations"].(map[string]interface{})[nodeInitialUnschedulableAnnotationKey])
	})
}

func TestCordonUncordonNodeRetriesOnConflict(t *testing.T) {
	originalBackoff := nodeUpdateBackoff
	nodeUpdateBackoff.Duration = 0
	nodeUpdateBackoff.Steps = 2
	t.Cleanup(func() {
		nodeUpdateBackoff = originalBackoff
	})

	testCases := []struct {
		name   string
		cordon bool
	}{
		{name: "cordon", cordon: true},
		{name: "uncordon"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			node := newTestNode(!tc.cordon, nil)
			if !tc.cordon {
				node.Annotations = map[string]string{nodeInitialUnschedulableAnnotationKey: "false"}
			}
			client, clientset := newTestClient(node)

			conflicts := 0
			clientset.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				if conflicts == 0 {
					conflicts++
					return true, nil, apierrors.NewConflict(
						corev1.Resource("nodes"), testNodeName, errors.New("object was modified"),
					)
				}
				return false, nil, nil
			})

			var err error
			if tc.cordon {
				err = client.CordonNode(testNodeName)
			} else {
				err = client.UncordonNode(testNodeName)
			}
			require.NoError(t, err)
			require.Equal(t, 1, conflicts)
			require.Len(t, getNodePatches(t, clientset), 2)
			require.Equal(t, tc.cordon, getTestNode(t, clientset).Spec.Unschedulable)
		})
	}
}

func TestNodeSchedulingStatePatchPreservesOtherAnnotations(t *testing.T) {
	const (
		existingAnnotation = "example.com/existing"
		existingValue      = "value"
	)
	client, clientset := newTestClient(newTestNode(false, map[string]string{
		existingAnnotation: existingValue,
	}))

	require.NoError(t, client.CordonNode(testNodeName))
	require.NoError(t, client.UncordonNode(testNodeName))

	node := getTestNode(t, clientset)
	require.Equal(t, existingValue, node.Annotations[existingAnnotation])
	require.NotContains(t, node.Annotations, nodeInitialUnschedulableAnnotationKey)
}

func TestUncordonNodePatchFailurePreservesState(t *testing.T) {
	client, clientset := newTestClient(newTestNode(
		true,
		map[string]string{nodeInitialUnschedulableAnnotationKey: "false"},
	))
	clientset.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("patch failed")
	})

	require.Error(t, client.UncordonNode(testNodeName))
	node := getTestNode(t, clientset)
	require.True(t, node.Spec.Unschedulable)
	require.Equal(t, "false", node.Annotations[nodeInitialUnschedulableAnnotationKey])
}

func TestCordonUncordonNodeRejectsInvalidRecordedState(t *testing.T) {
	testCases := []struct {
		name   string
		cordon bool
	}{
		{name: "cordon", cordon: true},
		{name: "uncordon"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client, clientset := newTestClient(newTestNode(
				true,
				map[string]string{nodeInitialUnschedulableAnnotationKey: "invalid"},
			))

			var err error
			if tc.cordon {
				err = client.CordonNode(testNodeName)
			} else {
				err = client.UncordonNode(testNodeName)
			}
			require.ErrorContains(t, err, "invalid value")
			require.Empty(t, getNodePatches(t, clientset))
		})
	}
}
