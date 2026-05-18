// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iotagrpc

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/iotaledger/hive.go/log"

	ledger_service "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/ledger_service"

)

const defaultBuf = 64

// StreamClient is a generic, reconnecting gRPC server-stream consumer.
type StreamClient[T any] struct {
	name    string
	address string
	options []grpc.DialOption
	logger  log.Logger

	conn *grpc.ClientConn

	// openStream uses conn to build the correct service client and open the server stream.
	// It returns a Recv function that yields messages of type T.
	openStream func(ctx context.Context, conn *grpc.ClientConn) (recv func() (T, error), err error)

	events chan T
	wg     sync.WaitGroup

	reconnectInterval time.Duration
	maxReconnectDelay time.Duration
}

func newStreamClient[T any](
	name string,
	address string,
	logger log.Logger,
	openStream func(ctx context.Context, conn *grpc.ClientConn) (recv func() (T, error), err error),
	opts ...grpc.DialOption,
) *StreamClient[T] {
	defaultOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             10 * time.Second,
				PermitWithoutStream: true,
			},
		),
	}
	return &StreamClient[T]{
		name:              name,
		address:           address,
		options:           append(defaultOpts, opts...),
		logger:            logger,
		events:            make(chan T, defaultBuf),
		reconnectInterval: 5 * time.Second,
		maxReconnectDelay: 30 * time.Second,
		openStream:        openStream,
	}
}

func (c *StreamClient[T]) Start(ctx context.Context) <-chan T {
	c.wg.Add(1)
	go c.run(ctx)
	return c.events
}

func (c *StreamClient[T]) WaitUntilStopped() {
	c.wg.Wait()
}

func (c *StreamClient[T]) run(ctx context.Context) {
	defer c.wg.Done()
	defer close(c.events)

	if err := c.dial(); err != nil {
		c.logger.LogErrorf("%s: initial gRPC dial failed: %v", c.name, err)
	}

	backoff := c.reconnectInterval

	for {
		if ctx.Err() != nil {
			break
		}

		if c.conn == nil {
			if !sleepCtx(ctx, backoff) {
				break
			}
			backoff = c.nextBackoff(backoff)
			_ = c.dial()
			continue
		}

		recv, err := c.openStream(ctx, c.conn)
		if err != nil {
			c.logger.LogErrorf("%s: failed to open stream: %v", c.name, err)
			if !sleepCtx(ctx, backoff) {
				break
			}
			backoff = c.nextBackoff(backoff)
			continue
		}

		c.logger.LogInfof("%s: stream established", c.name)
		backoff = c.reconnectInterval

		for {
			if ctx.Err() != nil {
				goto done
			}
			msg, err := recv()
			if err == io.EOF {
				c.logger.LogWarnf("%s: stream ended by server", c.name)
				break
			}
			if err != nil {
				c.logger.LogErrorf("%s: stream receive error: %v", c.name, err)
				break
			}
			select {
			case c.events <- msg:
			case <-ctx.Done():
				goto done
			}
		}

		if !sleepCtx(ctx, backoff) {
			break
		}
		backoff = c.nextBackoff(backoff)
	}
done:
}

func (c *StreamClient[T]) dial() error {
	if c.conn != nil {
		return nil
	}
	conn, err := grpc.NewClient(c.address, c.options...)
	if err != nil {
		c.logger.LogErrorf("%s: failed to connect to %s: %v", c.name, c.address, err)
		return err
	}
	c.conn = conn
	c.logger.LogInfof("%s: connected to gRPC at %s", c.name, c.address)
	return nil
}

func (c *StreamClient[T]) nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > c.maxReconnectDelay {
		return c.maxReconnectDelay
	}
	return next
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

// NewCheckpointStreamClient creates a StreamClient that streams CheckpointData
// from LedgerService.StreamCheckpoints with the given request.
func NewCheckpointStreamClient(
	address string,
	req *ledger_service.StreamCheckpointsRequest,
	logger log.Logger,
	opts ...grpc.DialOption,
) *StreamClient[*ledger_service.CheckpointData] {
	return newStreamClient[*ledger_service.CheckpointData](
		"checkpoint-stream",
		address,
		logger,
		func(ctx context.Context, conn *grpc.ClientConn) (func() (*ledger_service.CheckpointData, error), error) {
			cli := ledger_service.NewLedgerServiceClient(conn)
			stream, err := cli.StreamCheckpoints(ctx, req)
			if err != nil {
				return nil, err
			}
			return stream.Recv, nil
		},
		opts...,
	)
}
