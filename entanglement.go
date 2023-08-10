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
	peerID         peerID
	peerIO         PeerIO
	readerPipeline *peerReaderPipeline
	writerPipeline *peerWriterPipeline

	errCh <-chan error
}

func newPeer(
	peerID peerID,
	peerIO PeerIO,
	sendCh <-chan []byte,
	msgHandler MessageHandler,
	peers *peerSet,
	dedup *dedupCache,

) *peer {
	errCh := make(chan error, 1)
	pingTime := new(uint64)
	return &peer{
		peerID:         peerID,
		peerIO:         peerIO,
		readerPipeline: newPeerReaderPipeline(peerID, peerIO, msgHandler, peers, dedup, errCh, pingTime),
		writerPipeline: newPeerWriterPipeline(peerID, peerIO, sendCh, peers, dedup, errCh, pingTime),
		errCh:          errCh,
	}
}

func (p *peer) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("closer", parallel.Fail, func(ctx context.Context) error {
			var err error

			select {
			case <-ctx.Done():
				err = errors.WithStack(ctx.Err())
			case err = <-p.errCh:
			}

			_ = p.peerIO.Close()
			return err
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

	errCh    chan<- error
	pingTime *uint64
}

func newPeerReaderPipeline(
	peerID peerID,
	peerIO PeerIO,
	msgHandler MessageHandler,
	peers *peerSet,
	dedup *dedupCache,
	errCh chan<- error,
	pingTime *uint64,
) *peerReaderPipeline {
	return &peerReaderPipeline{
		peerID:     peerID,
		peerIO:     peerIO,
		msgHandler: msgHandler,
		peers:      peers,
		dedup:      dedup,
		errCh:      errCh,
		pingTime:   pingTime,
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
			defer close(ch01To02)

			for {
				msg, ok, err := prp.step01ReceiveMessage(messageBufferPool)
				if err != nil {
					return err
				}
				if ok {
					ch01To02 <- msg
				}
			}
		})
		spawn("step02HashMessages", parallel.Fail, func(ctx context.Context) error {
			defer close(ch02To03)

			for msg := range ch01To02 {
				ch02To03 <- prp.step02HashMessage(msg)
			}

			return errors.WithStack(ctx.Err())
		})
		spawn("step03DedupMessages", parallel.Fail, func(ctx context.Context) error {
			defer close(ch03To04)

			for msg := range ch02To03 {
				if prp.step03DedupMessage(msg, messageBufferPool) {
					ch03To04 <- msg
				}
			}

			return errors.WithStack(ctx.Err())
		})
		spawn("step04CopyMessages", parallel.Fail, func(ctx context.Context) error {
			defer close(ch04To05)

			var buffer []byte
			for msg := range ch03To04 {
				msg.Buffer, buffer = prp.step04CopyMessage(msg, buffer, messageBufferPool)
				ch04To05 <- msg
			}

			return errors.WithStack(ctx.Err())
		})
		spawn("step05BroadcastMessages", parallel.Fail, func(ctx context.Context) error {
			defer close(ch05To06)

			for msg := range ch04To05 {
				prp.step05BroadcastMessage(msg.Buffer)
				ch05To06 <- msg
			}

			return errors.WithStack(ctx.Err())
		})
		spawn("step06HandleMessages", parallel.Fail, func(ctx context.Context) error {
			for msg := range ch05To06 {
				if err := prp.step06HandleMessage(ctx, msg.Buffer, msg.Hash); err != nil {
					reportError(err, prp.errCh)
					break
				}
			}

			for range ch05To06 {
			}

			return errors.WithStack(ctx.Err())
		})
		return nil
	})
}

func (prp *peerReaderPipeline) step01ReceiveMessage(messageBufferPool chan []byte) (retRecvMsg receivedMessage, retOK bool, retErr error) {
	msgSize, err := binary.ReadUvarint(prp.peerIO)
	if err != nil {
		return receivedMessage{}, false, errors.WithStack(err)
	}

	atomic.StoreUint64(prp.pingTime, uint64(time.Now().Unix()))

	if msgSize == 0 {
		return receivedMessage{}, false, nil
	}

	if msgSize > maxMsgSize {
		return receivedMessage{}, false, errors.Errorf("message is too big: %d bytes, but %d is the maximum", msgSize, maxMsgSize)
	}

	messageBuffer := <-messageBufferPool
	if uint64(len(messageBuffer)) < msgSize {
		messageBuffer = make([]byte, msgSize)
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

func (prp *peerReaderPipeline) step03DedupMessage(msg receivedMessage, messageBufferPool chan []byte) bool {
	if prp.dedup.Check(msg.Hash) {
		messageBufferPool <- msg.Buffer
		return false
	}

	return true
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
	peers  *peerSet
	dedup  *dedupCache

	errCh    chan<- error
	pingTime *uint64
}

func newPeerWriterPipeline(
	peerID peerID,
	peerIO PeerIO,
	sendCh <-chan []byte,
	peers *peerSet,
	dedup *dedupCache,
	errCh chan<- error,
	pingTime *uint64,
) *peerWriterPipeline {
	return &peerWriterPipeline{
		peerID:   peerID,
		peerIO:   peerIO,
		sendCh:   sendCh,
		peers:    peers,
		dedup:    dedup,
		errCh:    errCh,
		pingTime: pingTime,
	}
}

func (pwp *peerWriterPipeline) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		ch01To02 := make(chan sendMessage, 10)
		ch02To03 := make(chan []byte, 10)

		spawn("closer", parallel.Fail, func(ctx context.Context) error {
			ticker := time.NewTicker(pingInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					pwp.peers.Delete(pwp.peerID)
					return errors.WithStack(ctx.Err())
				case <-ticker.C:
					select {
					case ch02To03 <- nil:
					default:
					}
				}
			}
		})
		spawn("step01HashMessage", parallel.Fail, func(ctx context.Context) error {
			defer close(ch01To02)

			for msg := range pwp.sendCh {
				ch01To02 <- pwp.step01HashMessage(msg)
			}

			return errors.WithStack(ctx.Err())
		})
		spawn("step02SetDedups", parallel.Fail, func(ctx context.Context) error {
			defer close(ch02To03)

			for sendMsg := range ch01To02 {
				ch02To03 <- pwp.step02SetDedup(sendMsg)
			}

			return errors.WithStack(ctx.Err())
		})
		spawn("step03WriteMessages", parallel.Fail, func(ctx context.Context) error {
			for msg := range ch02To03 {
				if msg == nil {
					if err := pwp.ping(); err != nil {
						reportError(err, pwp.errCh)
						//break
					}
				} else {
					if err := pwp.step03WriteMessage(msg); err != nil {
						reportError(err, pwp.errCh)
						break
					}
				}
			}

			for range ch02To03 {
			}

			return errors.WithStack(ctx.Err())
		})

		return nil
	})
}

func (pwp *peerWriterPipeline) ping() error {
	pingTime := atomic.LoadUint64(pwp.pingTime)
	if uint64(time.Now().Unix())-pingTime > missedPings*uint64(pingInterval/time.Second) {
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

	if sendCh, exists := ps.peers[pID]; exists {
		delete(ps.peers, pID)
		close(sendCh)
	}

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

func reportError(err error, errCh chan<- error) {
	select {
	case errCh <- err:
	default:
	}
}
