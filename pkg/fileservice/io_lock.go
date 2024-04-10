// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type IOLockKey struct {
	Path   string
	Offset int64
	End    int64
}

type IOLocks struct {
	mu    sync.Mutex
	locks map[IOLockKey]chan struct{}
}

func NewIOLocks() *IOLocks {
	return &IOLocks{
		locks: make(map[IOLockKey]chan struct{}),
	}
}

var slowIOWaitDuration = time.Second * 10

func (i *IOLocks) Lock(key IOLockKey) (unlock func(), wait func()) {
	i.mu.Lock()
	defer i.mu.Unlock()

	ch, ok := i.locks[key]

	if ok {
		// not locked
		wait = func() {
			t0 := time.Now()
			for {
				timer := time.NewTimer(slowIOWaitDuration)
				select {
				case <-ch:
					timer.Stop()
					return
				case <-timer.C:
					logutil.Warn("wait io lock for too long",
						zap.Any("wait", time.Since(t0)),
						zap.Any("key", key),
					)
				}
			}
		}
		return
	}

	// locked
	ch = make(chan struct{})
	i.locks[key] = ch
	unlock = func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		delete(i.locks, key)
		close(ch)
	}

	return
}
