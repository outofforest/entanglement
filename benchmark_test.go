package entanglement

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

// go test -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkHashBlake2b(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const (
		nItems = 1000
		size   = 10240
	)

	requireT := require.New(b)

	items := make([][]byte, 0, nItems)
	for i := 0; i < nItems; i++ {
		item := make([]byte, size)
		_, err := rand.Read(item)
		requireT.NoError(err)
		items = append(items, item)
	}

	for bi := 0; bi < b.N; bi++ {
		b.StartTimer()
		for _, item := range items {
			blake2b.Sum256(item)
		}
		b.StopTimer()
	}
}

func BenchmarkHashSha256(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const (
		nItems = 1000
		size   = 10240
	)

	requireT := require.New(b)

	items := make([][]byte, 0, nItems)
	for i := 0; i < nItems; i++ {
		item := make([]byte, size)
		_, err := rand.Read(item)
		requireT.NoError(err)
		items = append(items, item)
	}

	for bi := 0; bi < b.N; bi++ {
		b.StartTimer()
		for _, item := range items {
			sha256.Sum256(item)
		}
		b.StopTimer()
	}
}

func BenchmarkHashXXHash(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const (
		nItems = 1000
		size   = 10240
	)

	requireT := require.New(b)

	items := make([][]byte, 0, nItems)
	for i := 0; i < nItems; i++ {
		item := make([]byte, size)
		_, err := rand.Read(item)
		requireT.NoError(err)
		items = append(items, item)
	}

	for bi := 0; bi < b.N; bi++ {
		b.StartTimer()
		for _, item := range items {
			xxhash.Sum64(item)
		}
		b.StopTimer()
	}
}

func BenchmarkHashBlake2bReuse(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const (
		nItems = 1000
		size   = 10240
	)

	requireT := require.New(b)

	items := make([][]byte, 0, nItems)
	for i := 0; i < nItems; i++ {
		item := make([]byte, size)
		_, err := rand.Read(item)
		requireT.NoError(err)
		items = append(items, item)
	}

	for bi := 0; bi < b.N; bi++ {
		hasher, err := blake2b.New256(nil)
		requireT.NoError(err)
		buf := make([]byte, blake2b.Size)

		b.StartTimer()
		for _, item := range items {
			hasher.Reset()
			hasher.Write(item)
			hasher.Sum(buf[:1])
		}
		b.StopTimer()
	}
}

func BenchmarkHashSha256Reuse(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const (
		nItems = 1000
		size   = 10240
	)

	requireT := require.New(b)

	items := make([][]byte, 0, nItems)
	for i := 0; i < nItems; i++ {
		item := make([]byte, size)
		_, err := rand.Read(item)
		requireT.NoError(err)
		items = append(items, item)
	}

	for bi := 0; bi < b.N; bi++ {
		hasher := sha256.New()
		buf := make([]byte, sha256.Size)

		b.StartTimer()
		for _, item := range items {
			hasher.Reset()
			hasher.Write(item)
			hasher.Sum(buf[:1])
		}
		b.StopTimer()
	}
}

func BenchmarkHashXXHashReuse(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const (
		nItems = 1000
		size   = 10240
	)

	requireT := require.New(b)

	items := make([][]byte, 0, nItems)
	for i := 0; i < nItems; i++ {
		item := make([]byte, size)
		_, err := rand.Read(item)
		requireT.NoError(err)
		items = append(items, item)
	}

	for bi := 0; bi < b.N; bi++ {
		hasher := xxhash.New()
		buf := make([]byte, 8)

		b.StartTimer()
		for _, item := range items {
			hasher.Reset()
			_, _ = hasher.Write(item)
			hasher.Sum(buf[:1])
		}
		b.StopTimer()
	}
}

func BenchmarkMemory(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	const (
		nPeers    = 64
		nMessages = 10000
		msgSize   = 4096
	)

	requireT := require.New(b)

	for bi := 0; bi < b.N; bi++ {
		peers := make([]*peerIO, 0, nPeers)
		peerCh := make(chan PeerIO, nPeers)
		for i := 0; i < nPeers; i++ {
			pIO := newPeerIO()
			peers = append(peers, pIO)
			peerCh <- pIO
		}

		messages := make([][]byte, 0, nMessages)
		for i := 0; i < nMessages; i++ {
			msg := make([]byte, msgSize)
			_, err := rand.Read(msg)
			requireT.NoError(err)
			messages = append(messages, msg)
		}

		sendCh := make(chan []byte, nMessages)
		for i := 0; i < nMessages; i++ {
			msg := make([]byte, msgSize)
			_, err := rand.Read(msg)
			requireT.NoError(err)
			sendCh <- msg
		}

		ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
		b.Cleanup(cancel)

		received := new(uint64)
		eDoneCh := make(chan struct{})
		e := New[uint64](peerCh, sendCh, hashingFunc, func(ctx context.Context, msg []byte, hash uint64) error {
			if atomic.AddUint64(received, 1) == nMessages {
				close(eDoneCh)
			}
			return nil
		})

		peersDone := make(chan struct{}, nPeers)
		err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
			spawn("watchdog", parallel.Exit, func(ctx context.Context) error {
				for i := 0; i < nPeers; i++ {
					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case <-peersDone:
					}
				}

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case <-eDoneCh:
				}
				return nil
			})
			spawn("peers", parallel.Continue, func(ctx context.Context) error {
				return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
					for i, pIO := range peers {
						pIO := pIO
						spawn(fmt.Sprintf("peer-%d", i), parallel.Continue, func(ctx context.Context) error {
							return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
								spawn("write", parallel.Continue, func(ctx context.Context) error {
									defer func() {
										peersDone <- struct{}{}
									}()

									varintBuf := make([]byte, binary.MaxVarintLen64)
									for _, msg := range messages {
										n := binary.PutUvarint(varintBuf, uint64(len(msg)))
										_, err := pIO.ReadData.Write(varintBuf[:n])
										if err != nil {
											return errors.WithStack(err)
										}

										_, err = pIO.ReadData.Write(msg)
										if err != nil {
											return errors.WithStack(err)
										}
									}
									return nil
								})
								spawn("read", parallel.Continue, func(ctx context.Context) error {
									_, _ = pIO.WriteData.WriteTo(io.Discard)
									return nil
								})
								return nil
							})
						})
					}
					return nil
				})
			})

			b.StartTimer()
			spawn("entanglement", parallel.Fail, e.Run)
			return nil
		})
		b.StopTimer()
		requireT.NoError(err)
	}
}
