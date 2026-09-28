package stream

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/bilirec/bilirec/pkg/pool"
	"github.com/bilirec/bilirec/pkg/rw"
	"github.com/go-resty/resty/v2"
)

func (r *Service) ReadFlvStream(
	resp *resty.Response,
	ctx context.Context,
	qn int,
	chunkPool *pool.BucketedBytesPool,
	releasePool func(),
) (<-chan []byte, error) {
	readSize := r.readBufSizeForQn(qn)
	ch := make(chan []byte, r.chanBufferSizeForQn(qn))
	go r.readFlv(ch, resp.RawBody(), ctx, chunkPool, readSize, releasePool)
	return ch, nil
}

func (r *Service) readFlv(
	ch chan<- []byte,
	stream io.ReadCloser,
	ctx context.Context,
	chunkPool *pool.BucketedBytesPool,
	readSize int,
	releasePool func(),
) {
	stream = rw.NewIdleTimeoutReadCloser(stream, r.streamIdleTimeout)
	defer stream.Close()
	defer close(ch)
	defer releasePool()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			buf := chunkPool.GetSized(readSize)
			n, err := stream.Read(buf)
			if err == io.EOF {
				log.Info("直播流已结束")
				r.putChunk(chunkPool, buf)
				return
			} else if err != nil {
				if errors.Is(err, rw.ErrIdleTimeout) {
					log.Warnf("直播流超过 %v 没有收到数据，关闭连接", r.streamIdleTimeout)
				} else if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
					log.Errorf("读取直播流失败：%v", err)
				}
				r.putChunk(chunkPool, buf)
				return
			}
			if n > 0 {
				select {
				case ch <- buf[:n]:
				case <-ctx.Done():
					r.putChunk(chunkPool, buf)
					return
				}
			} else {
				r.putChunk(chunkPool, buf)
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}
