package drpc

import (
	"context"
	"log"

	"github.com/cockroachdb/cockroach/pkg/rpc/drpc/greeterpb"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmigrate"
)

type UnaryClientInterceptor func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, enc drpc.Encoding, next UnaryInvoker) error

type UnaryInvoker func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, enc drpc.Encoding) error

// new folder for interceptors in drpc fork.

// type of interceptor definition
// add options to ClientConn wrapper.
// ClientConn should work with both drpcconn.Conn and drpcpool.Conn

// modify drpc pool so that we give our ClientConn object (haves list of interceptors, grabs connection from pool, etc. functionality), and it
// should work with this.

// server interceptors.
// define type

// -------------  list of commits order. IN drpc repo
// interdceptor definitions in one commit
// client connection object with dial options
// add tests to use interceptors, ensure they are running

// Then we can start porting interceptors

//type StreamClientInterceptor func(ctx context.Context, rpc drpc.Description, next drpc.UnaryInvoker) error

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

func TestFunction() error {
	ctx := context.Background()
	clientConn, err := Dial(
		"localhost:9090",
		WithUnaryInterceptor(logUnaryInterceptor() /*, authUnaryInterceptor*/),
		/*WithStreamInterceptor(logStreamInterceptor),*/
	)
	if err != nil {
		log.Fatal(err)
	}

	client := greeterpb.NewDRPCGreeterClient(clientConn)
	req := &greeterpb.HelloRequest{
		Name: "World",
	}
	// current way of invocation using generated drpc client
	resp, err := client.SayHello(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Response: %s", resp.Message)
	// previous way of invocation where we would need to know function name
	//clientConn.Invoke(ctx, "TestFunction", nil, nil)

	return nil
}

// ensure that the interceptor builder function just takes only the necessary arguments.
// Don't take in in,out,cc,next etc. functions.
// Refer the example line 621 in rpc/context.go
func logUnaryInterceptor() UnaryClientInterceptor {
	return func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, enc drpc.Encoding, next UnaryInvoker) error {
		log.Printf("Starting RPC: %s", rpc)
		err := next(ctx, rpc, in, out, cc, enc)
		if err != nil {
			log.Printf("RPC %s failed: %v", rpc, err)
		} else {
			log.Printf("RPC %s succeeded", rpc)
		}
		return err
	}
}

// ------------------- inspired from grpc code
// chainUnaryClientInterceptors chains all unary client interceptors into one.
func chainUnaryClientInterceptors(cc *ClientConn) {
	interceptors := cc.dopts.chainUnaryInts
	var chainedInt UnaryClientInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, method string, in, out drpc.Message, cc *ClientConn, enc drpc.Encoding, invoker UnaryInvoker) error {
			return interceptors[0](ctx, method, in, out, cc, enc, getChainUnaryInvoker(interceptors, 0, invoker))
		}
	}
	cc.dopts.unaryInt = chainedInt
}

// getChainUnaryInvoker recursively generate the chained unary invoker.
// Update this to a for loop.
func getChainUnaryInvoker(
	interceptors []UnaryClientInterceptor, curr int, finalInvoker UnaryInvoker,
) UnaryInvoker {
	if curr == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, method string, in, out drpc.Message, cc *ClientConn, enc drpc.Encoding) error {
		return interceptors[curr+1](ctx, method, in, out, cc, enc, getChainUnaryInvoker(interceptors, curr+1, finalInvoker))
	}
}
