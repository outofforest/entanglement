package entanglement

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/outofforest/spin"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	pingInterval = time.Second
	missedPings  = 5
	maxMsgSize   = 2 * math.MaxUint16
	maxDedupSize = 1000
)

type MessageHandler func(ctx context.Context, msg []byte, hash [sha256.Size]byte) error

type PeerIO interface {
	io.ReadCloser
	io.ReaderFrom
}

type Entanglement struct {
	peerCh     <-chan PeerIO
	sendCh     <-chan []byte
	msgHandler MessageHandler
	peers      *peerSet
	dedup      *dedupCache
}

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

func (e *Entanglement) ClearCache(hash [sha256.Size]byte) {
	e.dedup.Remove(hash)
}

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
		writerPipeline: newPeerWriterPipeline(peerIO, sendCh, pingCount),
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
		readingBuffer := spin.New()

		messageBufferPool := make(chan []byte, 10)
		for i := 0; i < cap(messageBufferPool); i++ {
			messageBufferPool <- make([]byte, 4096)
		}

		ch01To02 := make(chan receivedMessage, 1)
		ch02To03 := make(chan receivedMessage, 1)
		ch03To04 := make(chan receivedMessage, 1)
		ch04To05 := make(chan receivedMessage, 1)

		spawn("closer", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			_ = readingBuffer.Close()
			return errors.WithStack(ctx.Err())
		})
		spawn("tapReceiveBytes", parallel.Fail, func(ctx context.Context) error {
			return prp.tapReceiveBytes(readingBuffer)
		})
		spawn("step01ReceiveMessages", parallel.Fail, func(ctx context.Context) error {
			for {
				msg, ok, err := prp.step01ReceiveMessage(ctx, readingBuffer, messageBufferPool)
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
		spawn("step02DedupMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch01To02:
				}

				msg, ok := prp.step02DedupMessage(msg, messageBufferPool)
				if ok {
					select {
					case <-ctx.Done():
						return errors.WithStack(ctx.Err())
					case ch02To03 <- msg:
					}
				}
			}
		})
		spawn("step03CopyMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch02To03:
				}

				msg.Buffer = prp.step03CopyMessage(msg, messageBufferPool)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case ch03To04 <- msg:
				}
			}
		})
		spawn("step04BroadcastMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch03To04:
				}

				prp.step04BroadcastMessage(msg.Buffer)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case ch04To05 <- msg:
				}
			}
		})
		spawn("step05HandleMessages", parallel.Fail, func(ctx context.Context) error {
			var msg receivedMessage

			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg = <-ch04To05:
				}

				if err := prp.step05HandleMessage(ctx, msg.Buffer, msg.Hash); err != nil {
					return err
				}
			}
		})
		return nil
	})
}

func (prp *peerReaderPipeline) tapReceiveBytes(r io.ReaderFrom) error {
	_, err := r.ReadFrom(prp.peerIO)
	return errors.WithStack(err)
}

func (prp *peerReaderPipeline) step01ReceiveMessage(ctx context.Context, r step01Reader, messageBufferPool chan []byte) (retRecvMsg receivedMessage, retOK bool, retErr error) {
	msgSize, err := binary.ReadUvarint(r)
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
	}

	defer func() {
		if !retOK {
			messageBufferPool <- messageBuffer
		}
	}()

	if uint64(len(messageBuffer)) < msgSize {
		messageBuffer = make([]byte, msgSize)
	}

	var nTotal uint64
	for nTotal < msgSize {
		n, err := r.Read(messageBuffer[nTotal:msgSize])
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

func (prp *peerReaderPipeline) step02DedupMessage(msg receivedMessage, messageBufferPool chan []byte) (receivedMessage, bool) {
	msg.Hash = sha256.Sum256(msg.Buffer[:msg.Size])

	if prp.dedup.Check(msg.Hash) {
		messageBufferPool <- msg.Buffer
		return receivedMessage{}, false
	}

	return msg, true
}

func (prp *peerReaderPipeline) step03CopyMessage(msg receivedMessage, messageBufferPool chan []byte) []byte {
	buf := make([]byte, msg.Size)
	copy(buf, msg.Buffer)

	messageBufferPool <- msg.Buffer

	return buf
}

func (prp *peerReaderPipeline) step04BroadcastMessage(msg []byte) {
	prp.peers.Broadcast(prp.peerID, msg)
}

func (prp *peerReaderPipeline) step05HandleMessage(ctx context.Context, msg []byte, hash [sha256.Size]byte) error {
	prp.peers.Broadcast(prp.peerID, msg)
	return prp.msgHandler(ctx, msg, hash)
}

type peerWriterPipeline struct {
	peerIO PeerIO
	sendCh <-chan []byte

	pingCount *uint64
}

func newPeerWriterPipeline(
	peerIO PeerIO,
	sendCh <-chan []byte,
	pingCount *uint64,
) *peerWriterPipeline {
	return &peerWriterPipeline{
		peerIO:    peerIO,
		sendCh:    sendCh,
		pingCount: pingCount,
	}
}

func (pwp *peerWriterPipeline) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		writingBuffer := spin.New()

		spawn("closer", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()
			_ = writingBuffer.Close()
			return errors.WithStack(ctx.Err())
		})
		spawn("step01WriteMessages", parallel.Fail, func(ctx context.Context) error {
			ticker := time.NewTicker(pingInterval)
			defer ticker.Stop()

			for {
				ticker.Reset(pingInterval)

				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case msg := <-pwp.sendCh:
					if err := pwp.step01WriteMessage(writingBuffer, msg); err != nil {
						return err
					}
				case <-ticker.C:
					if err := pwp.ping(writingBuffer); err != nil {
						return err
					}
				}
			}
		})
		spawn("drainWriteBytes", parallel.Fail, func(ctx context.Context) error {
			return pwp.drainWriteBytes(writingBuffer)
		})
		return nil
	})
}

func (pwp *peerWriterPipeline) ping(w io.Writer) error {
	prevPing := atomic.AddUint64(pwp.pingCount, 1)
	if prevPing > missedPings {
		return errors.New("connection is dead")
	}
	if _, err := w.Write([]byte{0x00}); err != nil {
		return err
	}

	return nil
}

func (pwp *peerWriterPipeline) step01WriteMessage(w io.Writer, msg []byte) error {
	varintBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(varintBuf, uint64(len(msg)))
	if _, err := w.Write(varintBuf[:n]); err != nil {
		return err
	}
	if _, err := w.Write(msg); err != nil {
		return err
	}
	return nil
}

func (pwp *peerWriterPipeline) drainWriteBytes(r io.Reader) error {
	_, err := pwp.peerIO.ReadFrom(r)
	return errors.WithStack(err)
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
	dedup1 map[[sha256.Size]byte]struct{}
	dedup2 map[[sha256.Size]byte]struct{}
}

func newDedupCache() *dedupCache {
	return &dedupCache{
		dedup1: make(map[[sha256.Size]byte]struct{}, maxDedupSize),
		dedup2: make(map[[sha256.Size]byte]struct{}, maxDedupSize),
	}
}

func (dc *dedupCache) Check(hash [sha256.Size]byte) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if _, exists := dc.dedup1[hash]; exists {
		return true
	}
	if len(dc.dedup1) == maxDedupSize {
		dc.dedup1 = dc.dedup2
		dc.dedup2 = make(map[[sha256.Size]byte]struct{}, maxDedupSize)
	}
	dc.dedup1[hash] = struct{}{}
	if len(dc.dedup1) > maxDedupSize/2 {
		dc.dedup2[hash] = struct{}{}
	}

	return false
}

func (dc *dedupCache) Remove(hash [sha256.Size]byte) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	delete(dc.dedup1, hash)
	delete(dc.dedup2, hash)
}

type receivedMessage struct {
	Buffer []byte
	Size   uint64
	Hash   [sha256.Size]byte
}

type step01Reader interface {
	io.Reader
	io.ByteReader
}
