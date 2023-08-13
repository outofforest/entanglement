package entanglement

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/spin"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newPeerIO() *peerIO {
	return &peerIO{
		ReadData:  spin.New(),
		WriteData: spin.New(),
	}
}

type peerIO struct {
	ReadData  *spin.Buffer
	WriteData *spin.Buffer
}

func (p *peerIO) Read(b []byte) (int, error) {
	return p.ReadData.Read(b)
}

func (p *peerIO) Write(b []byte) (int, error) {
	return p.WriteData.Write(b)
}

func (p *peerIO) ReadByte() (byte, error) {
	return p.ReadData.ReadByte()
}

func (p *peerIO) Close() error {
	if err := p.ReadData.Close(); err != nil {
		return err
	}
	return p.WriteData.Close()
}

func addMessages(requireT *require.Assertions, msgs ...[]byte) *bytes.Buffer {
	result := &bytes.Buffer{}
	varintBuf := make([]byte, binary.MaxVarintLen64)
	for _, m := range msgs {
		n := binary.PutUvarint(varintBuf, uint64(len(m)))
		_, err := result.Write(varintBuf[:n])
		requireT.NoError(err)

		_, err = result.Write(m)
		requireT.NoError(err)
	}
	return result
}

func hashingFunc(msg []byte) uint64 {
	return xxhash.Sum64(msg)
}

func TestThreeSimpleTalkers(t *testing.T) {
	requireT := require.New(t)

	peer1 := newPeerIO()
	_, err := peer1.ReadData.ReadFrom(addMessages(requireT,
		[]byte{0x00, 0x00},
		[]byte{0x01, 0x01},
	))
	requireT.NoError(err)

	peer2 := newPeerIO()
	_, err = peer2.ReadData.ReadFrom(addMessages(requireT,
		[]byte{0x02, 0x02},
		[]byte{0x03, 0x03},
	))
	requireT.NoError(err)

	peer3 := newPeerIO()
	_, err = peer3.ReadData.ReadFrom(addMessages(requireT,
		[]byte{0x04, 0x04},
		[]byte{0x05, 0x05},
	))
	requireT.NoError(err)

	peerCh := make(chan PeerIO, 3)
	peerCh <- peer1
	peerCh <- peer2
	peerCh <- peer3

	receivedCh := make(chan []byte, 6)
	e := New[uint64](peerCh, nil, hashingFunc, func(ctx context.Context, msg []byte, hash uint64) error {
		receivedCh <- msg
		return nil
	})

	received := make([][]byte, 0, cap(receivedCh))
	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	t.Cleanup(cancel)
	requireT.NoError(parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("entanglement", parallel.Fail, e.Run)
		spawn("results", parallel.Exit, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg := <-receivedCh:
					received = append(received, msg)
					if len(received) == cap(received) {
						return nil
					}
				}
			}
		})
		return nil
	}))
	requireT.ElementsMatch([][]byte{
		{0x00, 0x00},
		{0x01, 0x01},
		{0x02, 0x02},
		{0x03, 0x03},
		{0x04, 0x04},
		{0x05, 0x05},
	}, received)
}
