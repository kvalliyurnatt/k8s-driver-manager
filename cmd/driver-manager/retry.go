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
	"errors"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"
)

// retryBackoff returns the exponential backoff used to retry transient
// Kubernetes API errors, bounded to the given number of attempts (at least one).
func retryBackoff(attempts int) wait.Backoff {
	if attempts < 1 {
		attempts = 1
	}
	return wait.Backoff{Duration: time.Second, Factor: 2.0, Jitter: 0.2, Steps: attempts}
}

// retryOnAnyError retries fn on any error using the given backoff, logging each
// failed attempt at action. A cancelled ctx ends the retry loop promptly instead
// of sleeping out the remaining backoff, and is joined with the last error
// rather than being reduced to it, which wait.Interrupted would otherwise
// conflate.
func retryOnAnyError(ctx context.Context, log *logrus.Logger, backoff wait.Backoff, action string, fn func() error) error {
	maxAttempts := backoff.Steps
	attempt := 0

	var lastErr error

	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(context.Context) (bool, error) {
		attempt++

		lastErr = fn()
		if lastErr == nil {
			return true, nil
		}
		logFailedAttempt(log, action, attempt, maxAttempts, lastErr)
		return false, nil
	})
	if err == nil {
		return nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(ctxErr, lastErr)
	}
	if lastErr != nil {
		return lastErr
	}
	return err
}

// logFailedAttempt reports a failed attempt at action, distinguishing a retry
// from the attempt that exhausts the budget so the final failure is not
// misreported as "retrying".
func logFailedAttempt(log *logrus.Logger, action string, attempt, maxAttempts int, err error) {
	if attempt < maxAttempts {
		log.Warnf("Failed to %s (attempt %d/%d), retrying: %v", action, attempt, maxAttempts, err)
		return
	}
	log.Errorf("Failed to %s (attempt %d/%d), giving up: %v", action, attempt, maxAttempts, err)
}
