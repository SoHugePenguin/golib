// Copyright 2017 fatedier, fatedier@gmail.com
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

package io

import (
	"github.com/SoHugePenguin/golib/crypto"
	"github.com/SoHugePenguin/golib/pool"
	"github.com/golang/snappy"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Join two io.ReadWriteCloser and do some operations.
func Join(c1 io.ReadWriteCloser, c2 io.ReadWriteCloser, inCount *int64, outCount *int64, inLimit *int64, outLimit *int64) (errors []error) {
	var wait sync.WaitGroup
	recordErrs := make([]error, 2)

	pipe := func(number int, to io.ReadWriteCloser, from io.ReadWriteCloser, count *int64, speedLimit *int64) {
		defer wait.Done()
		defer func() {
			_ = to.Close()
		}()
		defer func() {
			_ = from.Close()
		}()

		// 限速实现
		const bufSize = 16384
		buf := pool.GetBuf(bufSize) // 16 KB / per run
		defer pool.PutBuf(buf)

		for {
			// 最低限速至16KBps，受限于bufSize，如要减速至0，直接禁用proxy即可
			if speedLimit != nil && *speedLimit <= 0 {
				<-time.After(time.Second)
				continue
			}
			if speedLimit != nil {
				*speedLimit -= bufSize
			}
			n, err := from.Read(buf)
			if n > 0 {
				nw, ew := to.Write(buf[:n])
				if nw > 0 {
					atomic.AddInt64(count, int64(nw))
				}
				if ew != nil {
					recordErrs[number] = ew
					return
				}
				if nw != n {
					recordErrs[number] = io.ErrShortWrite
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					recordErrs[number] = err
				}
				return
			}
		}
	}

	wait.Add(2)

	go pipe(0, c1, c2, inCount, inLimit)
	go pipe(1, c2, c1, outCount, outLimit)
	wait.Wait()

	for _, e := range recordErrs {
		if e != nil {
			errors = append(errors, e)
		}
	}
	return errors
}

func WithEncryption(rwc io.ReadWriteCloser, key []byte) (io.ReadWriteCloser, error) {
	w, err := crypto.NewWriter(rwc, key)
	if err != nil {
		return nil, err
	}
	return WrapReadWriteCloser(crypto.NewReader(rwc, key), w, func() error {
		return rwc.Close()
	}), nil
}

func WithCompression(rwc io.ReadWriteCloser) io.ReadWriteCloser {
	sr := snappy.NewReader(rwc)
	sw := snappy.NewBufferedWriter(rwc)
	return WrapReadWriteCloser(sr, sw, func() error {
		_ = sw.Close()
		return rwc.Close()
	})
}

// WithCompressionFromPool will get snappy reader and writer from pool.
// You can recycle the snappy reader and writer by calling the returned recycle function, but it is not necessary.
func WithCompressionFromPool(rwc io.ReadWriteCloser) (out io.ReadWriteCloser, recycle func()) {
	sr := pool.GetSnappyReader(rwc)
	sw := pool.GetSnappyWriter(rwc)
	out = WrapReadWriteCloser(sr, sw, func() error {
		err := rwc.Close()
		return err
	})
	recycle = func() {
		pool.PutSnappyReader(sr)
		pool.PutSnappyWriter(sw)
	}
	return
}

type ReadWriteCloser struct {
	r       io.Reader
	w       io.Writer
	closeFn func() error

	closed bool
	mu     sync.Mutex
}

// WrapReadWriteCloser closeFn will be called only once
func WrapReadWriteCloser(r io.Reader, w io.Writer, closeFn func() error) io.ReadWriteCloser {
	return &ReadWriteCloser{
		r:       r,
		w:       w,
		closeFn: closeFn,
		closed:  false,
	}
}

func (rwc *ReadWriteCloser) Read(p []byte) (n int, err error) {
	return rwc.r.Read(p)
}

func (rwc *ReadWriteCloser) Write(p []byte) (n int, err error) {
	return rwc.w.Write(p)
}

func (rwc *ReadWriteCloser) Close() error {
	rwc.mu.Lock()
	if rwc.closed {
		rwc.mu.Unlock()
		return nil
	}
	rwc.closed = true
	rwc.mu.Unlock()

	if rwc.closeFn != nil {
		return rwc.closeFn()
	}
	return nil
}
