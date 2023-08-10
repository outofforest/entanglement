package entanglement

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	pingInterval = time.Second
	missedPings  = 5
	maxMsgSize   = 2 * math.MaxUint16
	maxDedupSize = 1000
)

// MessageHandler defines the function handling received messages.
type MessageHandler func(ctx context.Context, msg []byte, hash uint64) error

// PeerIO specifies the interface required from connected peers.
type PeerIO interface {
	io.Closer
	io.Reader
	io.Writer
	io.ByteReader
}

// Entanglement broadcasts messages between peers.
type Entanglement struct {
	peerCh     <-chan PeerIO
	sendCh     <-chan []byte
	msgHandler MessageHandler
	peers      *peerSet
	dedup      *dedupCache
}

// New creates new entanglement.
func New(
	peerCh <-chan PeerIO,
	sendCh <-chan []byte,
	msgHandler MessageHandler,
) *Entanglement {
	return &Entanglement{
		peerCh:     peerCh,
		sendCh:     sendCh,
		msgHandler: msgHandler,
		peers:      newPeerSet(),
		dedup:      newDedupCache(),
	}
}

// ClearCache removes hash from the deduplication cache.
func (e *Entanglement) ClearCache(hash uint64) {
	e.dedup.Remove(hash)
}

// Run runs the entanglement.
func (e *Entanglement) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("broadcast", parallel.Fail, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg, ok := <-e.sendCh:
					if !ok {
						return errors.New("channel closed")
					}
					e.peers.Broadcast(0, msg)
				}
			}
		})
		spawn("join", parallel.Fail, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case peerIO := <-e.peerCh:
					sendCh := make(chan []byte, 10)
					peerID := e.peers.Add(sendCh)

					spawn(fmt.Sprintf("peer-%d", peerID), parallel.Continue, func(ctx context.Context) error {
						defer e.peers.Delete(peerID)

						peer := newPeer(peerID, peerIO, sendCh, e.msgHandler, e.peers, e.dedup)
						if err := peer.Run(ctx); err != nil {
							logger.Get(ctx).Error("Peer failed", zap.Error(err))
						}
						return nil
					})
				}
			}
		})

		return nil
	})
}

type peer struct {
	peerIO         PeerIO
	readerPipeline *peerReaderPipeline
	writerPipeline *peerWriterPipeline
}

func newPeer(
	peerID peerID,
	peerIO PeerIO,
	sendCh <-chan []byte,
	msgHandler MessageHandler,
	peers *peerSet,
	dedup *dedupCache,

) *peer {
	pingCount := new(uint64)
	return &peer{
		peerIO:         peerIO,
		readerPipeline: newPeerReaderPipeline(peerID, peerIO, msgHandler, peers, dedup, pingCount),
		writerPipeline: newPeerWriterPipeline(peerID, peerIO, sendCh, dedup, pingCount),
	}
}

func (p *peer) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("closer", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			_ = p.peerIO.Close()
			return errors.WithStack(ctx.Err())
		})
		spawn("reader", parallel.Fail, p.readerPipeline.Run)
		spawn("writer", parallel.Fail, p.writerPipeline.Run)
		return nil
	})
}

type peerReaderPipeline struct {
	peerID     peerID
	peerIO     PeerIO
	msgHandler MessageHandler
	peers      *peerSet
	dedup      *dedupCache

	pingCount *uint64
}

func newPeerReaderPipeline(
	peerID peerID,
	peerIO PeerIO,
	msgHandler MessageHandler,
	peers *peerSet,
	dedup *dedupCache,
	pingCount *uint64,
) *peerReaderPipeline {
	return &peerReaderPipeline{
		peerID:     peerID,
		peerIO:     peerIO,
		msgHandler: msgHandler,
		peers:      peers,
		dedup:      dedup,
		pingCount:  pingCount,
	}
}

func (prp *peerReaderPipeline) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		messageBufferPool := make(chan []byte, 10)
		for i := 0; i < cap(messageBufferPool); i++ {
			messageBufferPool <- make([]byte, 4096)
		}

		ch01To02 := make(chan receivedMessage, 10)
		ch02To03 := make(chan receivedMessage, 10)
		ch03To04 := make(chan receivedMessage, 10)
		ch04To05 := make(chan receivedMessage, 10)
		ch05To06 := make(chan receivedMessage, 10)

		spawn("step01ReceiveMessages", parallel.Fail, func(ctx context.Context) error {
			for {
				msg, ok, err := prp.step01ReceiveMessage(ctx, messageBufferPool)
				if err != nil {
					return err
				}
				if ok {
					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case ch01To02 <- msg:
					}
				}
			}
		})
		spawn("step02HashMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch01To02:
				}

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case ch02To03 <- prp.step02HashMessage(msg):
				}
			}
		})
		spawn("step03DedupMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch02To03:
				}

				msg, ok := prp.step03DedupMessage(msg, messageBufferPool)
				if ok {
					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case ch03To04 <- msg:
					}
				}
			}
		})
		spawn("step04CopyMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage
			var buffer []byte

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch03To04:
				}

				msg.Buffer, buffer = prp.step04CopyMessage(msg, buffer, messageBufferPool)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case ch04To05 <- msg:
				}
			}
		})
		spawn("step05BroadcastMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch04To05:
				}

				prp.step05BroadcastMessage(msg.Buffer)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case ch05To06 <- msg:
				}
			}
		})
		spawn("step06HandleMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch05To06:
				}

				if err := prp.step06HandleMessage(ctx, msg.Buffer, msg.Hash); err != nil {
					return err
				}
			}
		})
		return nil
	})
}

func (prp *peerReaderPipeline) step01ReceiveMessage(ctx context.Context, messageBufferPool chan []byte) (retRecvMsg receivedMessage, retOK bool, retErr error) {
	msgSize, err := binary.ReadUvarint(prp.peerIO)
	if err != nil {
		return receivedMessage{}, false, errors.WithStack(err)
	}

	atomic.StoreUint64(prp.pingCount, 0)

	if msgSize == 0 {
		return receivedMessage{}, false, nil
	}

	if msgSize > maxMsgSize {
		return receivedMessage{}, false, errors.Errorf("message is too big: %d bytes, but %d is the maximum", msgSize, maxMsgSize)
	}

	var messageBuffer []byte
	select {
	case <-ctx.Done():
		return receivedMessage{}, false, errors.WithStack(ctx.Err())
	case messageBuffer = <-messageBufferPool:
		if uint64(len(messageBuffer)) < msgSize {
			messageBuffer = make([]byte, msgSize)
		}
	}
	defer func() {
		if !retOK {
			messageBufferPool <- messageBuffer
		}
	}()

	var nTotal uint64
	for nTotal < msgSize {
		n, err := prp.peerIO.Read(messageBuffer[nTotal:msgSize])
		if err != nil {
			return receivedMessage{}, false, err
		}
		nTotal += uint64(n)
	}

	return receivedMessage{
		Buffer: messageBuffer,
		Size:   msgSize,
	}, true, nil
}

func (prp *peerReaderPipeline) step02HashMessage(msg receivedMessage) receivedMessage {
	msg.Hash = xxhash.Sum64(msg.Buffer[:msg.Size])
	return msg
}

func (prp *peerReaderPipeline) step03DedupMessage(msg receivedMessage, messageBufferPool chan []byte) (receivedMessage, bool) {
	if prp.dedup.Check(msg.Hash) {
		messageBufferPool <- msg.Buffer
		return receivedMessage{}, false
	}

	return msg, true
}

func (prp *peerReaderPipeline) step04CopyMessage(msg receivedMessage, buffer []byte, messageBufferPool chan []byte) ([]byte, []byte) {
	if uint64(len(buffer)) < msg.Size {
		buffer = make([]byte, 2*maxMsgSize)
	}
	copy(buffer, msg.Buffer[:msg.Size])

	messageBufferPool <- msg.Buffer

	return buffer[:msg.Size], buffer[msg.Size:]
}

func (prp *peerReaderPipeline) step05BroadcastMessage(msg []byte) {
	prp.peers.Broadcast(prp.peerID, msg)
}

func (prp *peerReaderPipeline) step06HandleMessage(ctx context.Context, msg []byte, hash uint64) error {
	return prp.msgHandler(ctx, msg, hash)
}

type peerWriterPipeline struct {
	peerID peerID
	peerIO PeerIO
	sendCh <-chan []byte
	dedup  *dedupCache

	pingCount *uint64
}

func newPeerWriterPipeline(
	peerID peerID,
	peerIO PeerIO,
	sendCh <-chan []byte,
	dedup *dedupCache,
	pingCount *uint64,
) *peerWriterPipeline {
	return &peerWriterPipeline{
		peerID:    peerID,
		peerIO:    peerIO,
		sendCh:    sendCh,
		dedup:     dedup,
		pingCount: pingCount,
	}
}

func (pwp *peerWriterPipeline) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()

		ch01To02 := make(chan sendMessage, 10)
		ch02To03 := make(chan []byte, 10)

		spawn("step01HashMessage", parallel.Fail, func(ctx context.Context) error {
			var msg []byte

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-pwp.sendCh:
				}

				sendMsg := pwp.step01HashMessage(msg)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case ch01To02 <- sendMsg:
				}
			}
		})
		spawn("step02SetDedups", parallel.Fail, func(ctx context.Context) error {
			var msg []byte
			var sendMsg sendMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case sendMsg = <-ch01To02:
				}

				msg = pwp.step02SetDedup(sendMsg)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case ch02To03 <- msg:
				}
			}
		})
		spawn("step03WriteMessages", parallel.Fail, func(ctx context.Context) error {
			for {
				ticker.Reset(pingInterval)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg := <-ch02To03:
					if err := pwp.step03WriteMessage(msg); err != nil {
						return err
					}
				case <-ticker.C:
					if err := pwp.ping(); err != nil {
						return err
					}
				}
			}
		})

		return nil
	})
}

func (pwp *peerWriterPipeline) ping() error {
	prevPing := atomic.AddUint64(pwp.pingCount, 1)
	if prevPing > missedPings {
		return errors.New("connection is dead")
	}
	if _, err := pwp.peerIO.Write([]byte{0x00}); err != nil {
		return err
	}

	return nil
}

func (pwp *peerWriterPipeline) step01HashMessage(msg []byte) sendMessage {
	return sendMessage{
		Message: msg,
		Hash:    xxhash.Sum64(msg),
	}
}

func (pwp *peerWriterPipeline) step02SetDedup(msg sendMessage) []byte {
	pwp.dedup.Check(msg.Hash)
	return msg.Message
}

func (pwp *peerWriterPipeline) step03WriteMessage(msg []byte) error {
	varintBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(varintBuf, uint64(len(msg)))
	if _, err := pwp.peerIO.Write(varintBuf[:n]); err != nil {
		return err
	}
	if _, err := pwp.peerIO.Write(msg); err != nil {
		return err
	}
	return nil
}

type peerID uint64

type peerSet struct {
	mu    sync.RWMutex
	peers map[peerID]chan<- []byte
}

func newPeerSet() *peerSet {
	return &peerSet{
		peers: map[peerID]chan<- []byte{},
	}
}

func (ps *peerSet) Add(sendCh chan<- []byte) peerID {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for {
		id := peerID(rand.Int63n(math.MaxInt64)) + 1
		if _, exists := ps.peers[id]; !exists {
			ps.peers[id] = sendCh
			return id
		}
	}
}

func (ps *peerSet) Delete(pID peerID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.peers, pID)
}

func (ps *peerSet) Broadcast(senderID peerID, msg []byte) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for peerID, peer := range ps.peers {
		if peerID == senderID {
			continue
		}

		select {
		case peer <- msg:
		default:
		}
	}
}

type dedupCache struct {
	mu     sync.RWMutex
	dedup1 map[uint64]struct{}
	dedup2 map[uint64]struct{}
}

func newDedupCache() *dedupCache {
	return &dedupCache{
		dedup1: make(map[uint64]struct{}, maxDedupSize),
		dedup2: make(map[uint64]struct{}, maxDedupSize),
	}
}

func (dc *dedupCache) Check(hash uint64) bool {
	exists := func() bool {
		dc.mu.RLock()
		defer dc.mu.RUnlock()

		_, exists := dc.dedup1[hash]
		return exists
	}()
	if exists {
		return true
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	if _, exists := dc.dedup1[hash]; exists {
		return true
	}

	if len(dc.dedup1) == maxDedupSize {
		dc.dedup1 = dc.dedup2
		dc.dedup2 = make(map[uint64]struct{}, maxDedupSize)
	}
	dc.dedup1[hash] = struct{}{}
	if len(dc.dedup1) > maxDedupSize/2 {
		dc.dedup2[hash] = struct{}{}
	}

	return false
}

func (dc *dedupCache) Remove(hash uint64) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	delete(dc.dedup1, hash)
	delete(dc.dedup2, hash)
}

type receivedMessage struct {
	Buffer []byte
	Size   uint64
	Hash   uint64
}

type sendMessage struct {
	Message []byte
	Hash    uint64
}
