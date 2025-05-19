package drpc

import (
	"context"
	"log"

	"github.com/cockroachdb/cockroach/pkg/rpc/drpc/chatpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/drpc/greeterpb"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmigrate"
)

type UnaryClientInterceptor func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, enc drpc.Encoding, next UnaryInvoker) error

type UnaryInvoker func(ctx context.Context, rpc string, in, out drpc.Message, cc *ClientConn, enc drpc.Encoding) error

// Streamer is a function that opens a new DRPC stream.
type Streamer func(ctx context.Context, method string, conn *ClientConn) (drpc.Stream, error)

// StreamClientInterceptor is the DRPC equivalent of a gRPC stream client interceptor.
type StreamClientInterceptor func(ctx context.Context, method string, conn *ClientConn, streamer Streamer) (drpc.Stream, error)

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
	unaryInts  []UnaryClientInterceptor
	streamInts []StreamClientInterceptor
}

func WithChainUnaryInterceptor(ints ...UnaryClientInterceptor) DialOption {
	return func(opt *ClientConnOptions) {
		opt.unaryInts = append(opt.unaryInts, ints...)
	}
}

func WithChainStreamInterceptor(ints ...StreamClientInterceptor) DialOption {
	return func(opt *ClientConnOptions) {
		opt.streamInts = append(opt.streamInts, ints...)
	}
}

func createDRPCConnection(ctx context.Context, target string) (drpc.Conn, error) {
	// Dial Raw TCP connection
	rawConn, err := drpcmigrate.DialWithHeader(ctx, "tcp", target, drpcmigrate.DRPCHeader)
	if err != nil {
		return nil, err
	}
	// drpc connection
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

func TestStreamFunction() error {
	ctx := context.Background()
	clientConn, err := Dial(
		"localhost:9000",
		WithChainUnaryInterceptor(logUnaryInterceptor() /*, authUnaryInterceptor*/),
		WithChainStreamInterceptor(LogStreamInterceptor()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer clientConn.Close()

	// - Create the DRPC client
	client := chatpb.NewDRPCChatServiceClient(clientConn)
	// - Context with timeout
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()

	// - Open the bidirectional stream
	stream, err := client.ChatStream(ctx)
	if err != nil {
		log.Fatalf("ChatStream error: %v", err)
	}
	// - Send & receive a few messages
	for _, text := range []string{"Hi", "How are you?", "Bye!"} {
		if err := stream.Send(&chatpb.ChatMessage{Sender: "Client", Text: text}); err != nil {
			log.Fatalf("Send error: %v", err)
		}
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalf("Recv error: %v", err)
		}
		log.Printf("Server replied: %s", resp.Text)
	}
	return nil
}

func TestFunction() error {
	ctx := context.Background()
	clientConn, err := Dial(
		"localhost:9090",
		WithChainUnaryInterceptor(logUnaryInterceptor() /*, authUnaryInterceptor*/),
		/*WithChainStreamInterceptor(logStreamInterceptor()),*/
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

type loggingStream struct {
	drpc.Stream
}

func (s *loggingStream) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	log.Printf("[LOG ] → Sending message: %T %#v", msg, msg)
	return s.Stream.MsgSend(msg, enc)
}

func (s *loggingStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	err := s.Stream.MsgRecv(msg, enc)
	if err == nil {
		log.Printf("[LOG ] ← Received message: %T %#v", msg, msg)
	}
	return err
}

func LogStreamInterceptor() StreamClientInterceptor {
	return func(ctx context.Context, rpc string, cc *ClientConn, streamer Streamer) (drpc.Stream, error) {
		log.Printf("Starting stream: %s", rpc)
		stream, err := streamer(ctx, rpc, cc)
		if err != nil {
			log.Printf("Stream %s failed: %v", rpc, err)
			return nil, err
		}
		log.Printf("Stream %s succeeded", rpc)
		return &loggingStream{stream}, nil
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

// chainStreamClientInterceptors chains all stream client interceptors into one.
func chainStreamClientInterceptors(cc *ClientConn) {
	interceptors := cc.dopts.chainStreamInts
	var chainedInt StreamClientInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, method string, cc *ClientConn, streamer Streamer) (drpc.Stream, error) {
			return interceptors[0](ctx, method, cc, getChainStreamer(interceptors, 0, streamer))
		}
	}
	cc.dopts.streamInt = chainedInt
}

// getChainStreamer recursively generate the chained client stream constructor.
func getChainStreamer(
	interceptors []StreamClientInterceptor, curr int, finalStreamer Streamer,
) Streamer {
	if curr == len(interceptors)-1 {
		return finalStreamer
	}
	return func(ctx context.Context, method string, cc *ClientConn) (drpc.Stream, error) {
		return interceptors[curr+1](ctx, method, cc, getChainStreamer(interceptors, curr+1, finalStreamer))
	}
}
