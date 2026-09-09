//go:build !darwin && !windows

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

package main

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"
)

// fastBackoff keeps retries near-instant so tests do not sleep.
func fastBackoff(steps int) wait.Backoff {
	return wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: steps}
}

func discardLogger() *logrus.Logger {
	log := logrus.New()
	log.SetOutput(io.Discard)
	return log
}

func TestRetryOnAnyErrorRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	err := retryOnAnyError(context.Background(), discardLogger(), fastBackoff(5), "cordon node", func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("api-server timeout")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryOnAnyErrorReturnsLastErrorWhenExhausted(t *testing.T) {
	attempts := 0
	err := retryOnAnyError(context.Background(), discardLogger(), fastBackoff(3), "uncordon node", func() error {
		attempts++
		return fmt.Errorf("api-server down")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "api-server down")
	require.Equal(t, 3, attempts)
}

func TestRetryBackoffAlwaysAttemptsAtLeastOnce(t *testing.T) {
	require.GreaterOrEqual(t, retryBackoff(0).Steps, 1)
	require.Equal(t, 5, retryBackoff(5).Steps)
}

func TestRetryOnAnyErrorStopsPromptlyWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A backoff this long would hang the test if the remaining sleep were not
	// cut short by the cancelled context.
	backoff := wait.Backoff{Duration: time.Hour, Factor: 1.0, Steps: 5}

	attempts := 0
	start := time.Now()
	err := retryOnAnyError(ctx, discardLogger(), backoff, "uncordon node", func() error {
		attempts++
		cancel()
		return fmt.Errorf("api-server down")
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
	require.Less(t, time.Since(start), 30*time.Second)
}

func TestRetryOnAnyErrorJoinsCancellationWithLastError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sentinel := fmt.Errorf("api-server down")
	err := retryOnAnyError(ctx, discardLogger(), fastBackoff(5), "uncordon node", func() error {
		cancel()
		return sentinel
	})

	// errors.Join keeps both causes inspectable rather than flattening them
	// into a message.
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, sentinel)
}

func TestLogFailedAttemptOnlyPromisesARetryWhileAttemptsRemain(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	var levels []logrus.Level
	log.AddHook(collectLevels(&levels))

	logFailedAttempt(log, "cordon node", 1, 3, fmt.Errorf("boom"))
	logFailedAttempt(log, "cordon node", 3, 3, fmt.Errorf("boom"))

	require.Equal(t, []logrus.Level{logrus.WarnLevel, logrus.ErrorLevel}, levels)
}

type levelCollector struct{ levels *[]logrus.Level }

func (c levelCollector) Levels() []logrus.Level { return logrus.AllLevels }

func (c levelCollector) Fire(entry *logrus.Entry) error {
	*c.levels = append(*c.levels, entry.Level)
	return nil
}

func collectLevels(levels *[]logrus.Level) logrus.Hook { return levelCollector{levels: levels} }
