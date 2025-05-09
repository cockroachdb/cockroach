package drpc

import (
	"context"
	"log"

	"github.com/cockroachdb/cockroach/pkg/rpc/drpc/greeterpb"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmigrate"
)

type UnaryClientInterceptor func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, next HandlerFunc) error

type HandlerFunc func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn) error

//type StreamClientInterceptor func(ctx context.Context, rpc drpc.Description, next drpc.HandlerFunc) error

type ClientConn struct {
	conn           drpc.Conn // this is not same as the connection returned from drpcpool.
	enc            drpc.Encoding
	unaryIntercept UnaryClientInterceptor
	//streamInterceptor StreamClientInterceptor
}

type DialOption func(*ClientConnOptions)

type ClientConnOptions struct {
	unaryInts []UnaryClientInterceptor
	//streamInts []StreamClientInterceptor
}

func WithUnaryInterceptor(ints ...UnaryClientInterceptor) DialOption {
	return func(opt *ClientConnOptions) {
		opt.unaryInts = append(opt.unaryInts, ints...)
	}
}

/*func WithStreamInterceptor(ints ...StreamClientInterceptor) DialOption {
	return func(opt *ClientConnOptions) {
		opt.streamInts = append(opt.streamInts, ints...)
	}
}*/

func Dial(target string, opts ...DialOption) (*ClientConn, error) {
	ctx := context.Background()
	drpcConn, err := createDRPCConnection(ctx, target)
	if err != nil {
		return nil, err
	}

	clientOptions := applyDialOptions(opts)

	return &ClientConn{
		conn:           drpcConn,
		unaryIntercept: chainUnaryInterceptors(clientOptions.unaryInts...),
		//streamInterceptor: chainStreamInterceptors(clientOptions.streamInts...),
	}, nil
}

func createDRPCConnection(ctx context.Context, target string) (drpc.Conn, error) {
	rawConn, err := drpcmigrate.DialWithHeader(ctx, "tcp", target, drpcmigrate.DRPCHeader)
	if err != nil {
		return nil, err
	}
	dconn := drpcconn.New(rawConn)
	var conn drpc.Conn = dconn
	return conn, nil
}

func applyDialOptions(opts []DialOption) *ClientConnOptions {
	options := &ClientConnOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func chainUnaryInterceptors(interceptors ...UnaryClientInterceptor) UnaryClientInterceptor {
	return func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, next HandlerFunc) error {
		var chained HandlerFunc = func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn) error {
			return (cc.conn).Invoke(ctx, rpc, cc.enc, in, out)
		}
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn) error {
				return interceptor(ctx, rpc, in, out, cc, next)
			}
		}
		return chained(ctx, rpc, in, out, cc)
	}
}

func (cc *ClientConn) Invoke(ctx context.Context, method string, req, reply drpc.Message) error {
	return cc.unaryIntercept(ctx, method, req, reply, cc, func(ctx context.Context, method string, req, reply drpc.Message, cc *ClientConn) error {
		return (cc.conn).Invoke(ctx, method, cc.enc, req, reply)
	})
}

func testFunction() error {
	ctx := context.Background()
	//// dial the drpc server with the drpc connection header
	//rawconn, err := drpcmigrate.DialWithHeader(ctx, "tcp", "localhost:8080", drpcmigrate.DRPCHeader)
	//if err != nil {
	//	return err
	//}
	//// convert the net.Conn to a drpc.Conn
	//conn := drpcconn.New(rawconn)
	//defer conn.Close()
	//// create a new drpc client from this conn

	clientConn, err := Dial(
		"localhost:9000",
		WithUnaryInterceptor(logUnaryInterceptor /*authUnaryInterceptor*/),
		/*WithStreamInterceptor(logStreamInterceptor),*/
	)
	if err != nil {
		log.Fatal(err)
	}

	client := greeterpb.NewDRPCGreeterClient(clientConn)
	//clientConn.Invoke(ctx, "TestFunction", nil, nil)

	return nil
}

// ensure that the interceptor builder function just takes only the necessary arguments.
// Don't take in in,out,cc,next etc. functions.
// Refer the example line 621 in rpc/context.go
func logUnaryInterceptor(
	ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, next HandlerFunc,
) error {
	log.Printf("Starting RPC: %s", rpc)
	err := next(ctx, rpc, in, out, cc)
	if err != nil {
		log.Printf("RPC %s failed: %v", rpc, err)
	} else {
		log.Printf("RPC %s succeeded", rpc)
	}
	return err
}
