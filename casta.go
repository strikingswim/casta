package execution

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/weeaa/jito-go/clients/searcher_client"
	jito_pb "github.com/weeaa/jito-go/pb"
	"go.uber.org/zap"
)

type Submitter struct {
	client  *searcher_client.Client
	log     *zap.Logger
	sent    atomic.Uint64
	mu      sync.Mutex
	results map[string]state
}

type state uint8

const (
	statePending state = iota + 1
	stateLanded
	stateFailed
)

func NewSubmitter(ctx context.Context, log *zap.Logger, blockEngineURL string, jitoRPC, rpcClient *rpc.Client, key solana.PrivateKey) (*Submitter, error) {
	client, err := searcher_client.New(ctx, blockEngineURL, jitoRPC, rpcClient, key, &tls.Config{MinVersion: tls.VersionTLS12})
	if err != nil {
		return nil, fmt.Errorf("jito auth: %w", err)
	}

	s := &Submitter{client: client, log: log, results: make(map[string]state)}
	go s.readResults(ctx)
	return s, nil
}

func (s *Submitter) TipAccount() (solana.PublicKey, error) {
	addr, err := s.client.GetRandomTipAccount()
	if err != nil {
		return solana.PublicKey{}, err
	}
	return solana.PublicKeyFromBase58(addr)
}

func (s *Submitter) Submit(txs []*solana.Transaction) (string, error) {
	resp, err := s.client.SendBundle(txs)
	if err != nil {
		s.log.Error("bundle broadcast failed", zap.Error(err))
		return "", err
	}
	s.mu.Lock()
	s.results[resp.Uuid] = statePending
	s.mu.Unlock()
	s.sent.Add(1)
	s.log.Info("bundle sent", zap.String("uuid", resp.Uuid), zap.Int("txs", len(txs)))
	return resp.Uuid, nil
}

func (s *Submitter) readResults(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		sub, err := s.client.NewBundleSubscriptionResults()
		if err != nil {
			s.log.Warn("subscribe bundle results", zap.Error(err))
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 20*time.Second)
			continue
		}

		for {
			res, err := sub.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Warn("recv failed, will resubscribe", zap.Error(err))
				break
			}
			backoff = time.Second
			s.handle(res)
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, 20*time.Second)
	}
}

func (s *Submitter) handle(res *jito_pb.BundleResult) {
	id := res.BundleId
	switch res.Result.(type) {
	case *jito_pb.BundleResult_Processed, *jito_pb.BundleResult_Finalized:
		if s.record(id, stateLanded) {
			s.log.Info("bundle included", zap.String("id", id))
		}
	case *jito_pb.BundleResult_Rejected:
		if s.record(id, stateFailed) {
			s.log.Warn("bundle failed", zap.String("bundle", id), zap.String("reason", "rejected"))
		}
	case *jito_pb.BundleResult_Dropped:
		if s.record(id, stateFailed) {
			s.log.Warn("bundle failed", zap.String("bundle", id), zap.String("reason", "dropped"))
		}
	}
}

func (s *Submitter) record(id string, st state) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, known := s.results[id]; !known || cur != statePending {
		return false
	}
	s.results[id] = st
	return true
}

func (s *Submitter) LandRate() float64 {
	sent := s.sent.Load()
	if sent == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var landed uint64
	for _, st := range s.results {
		if st == stateLanded {
			landed++
		}
	}
	return float64(landed) / float64(sent)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
