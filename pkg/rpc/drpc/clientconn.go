package drpc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	storjdrpc "storj.io/drpc"
)

type ClientConn struct {
	conn storjdrpc.Conn // this is not same as the connection returned from drpcpool.
	// what if we wrap a connection pool here? Interchangable.
	//connGetter func(...) storjdrpc.Conn
	enc   storjdrpc.Encoding // not needed.
	dopts dialOptions        // Default and user specified dial options.
}

type dialOptions struct {
	unaryInt UnaryClientInterceptor
	//streamInt StreamClientInterceptor

	chainUnaryInts []UnaryClientInterceptor
	//chainStreamInts []StreamClientInterceptor
}

func (cc *ClientConn) Close() error {
	return cc.conn.Close()
}

func (cc *ClientConn) Closed() <-chan struct{} {
	return cc.conn.Closed()
}

func (cc *ClientConn) Invoke(
	ctx context.Context, rpc string, enc storjdrpc.Encoding, in, out storjdrpc.Message,
) error {
	// Use the interceptor chain if set, otherwise delegate directly
	log.Infof(ctx, "rpc.ClientConn.Invoke encoding: %v", enc)
	if cc.dopts.unaryInt != nil {
		return cc.dopts.unaryInt(ctx, rpc, in, out, cc, enc,
			func(ctx context.Context, rpc string, in, out storjdrpc.Message, cc *ClientConn, enc storjdrpc.Encoding) error {
				return cc.conn.Invoke(ctx, rpc, enc, in, out)
			})
	}
	return cc.conn.Invoke(ctx, rpc, enc, in, out)
}

func (cc *ClientConn) NewStream(
	ctx context.Context, rpc string, enc storjdrpc.Encoding,
) (storjdrpc.Stream, error) {
	return cc.conn.NewStream(ctx, rpc, enc)
}

func Dial(target string, opts ...DialOption) (*ClientConn, error) {
	ctx := context.Background()
	drpcConn, err := createDRPCConnection(ctx, target)
	if err != nil {
		return nil, err
	}

	clientOptions := applyDialOptions(opts)
	cc := &ClientConn{
		conn:  drpcConn,
		dopts: dialOptions{chainUnaryInts: clientOptions.unaryInts},
	}
	chainUnaryClientInterceptors(cc)
	return cc, nil
}
