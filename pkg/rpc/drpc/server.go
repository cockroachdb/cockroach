package drpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/cockroachdb/cockroach/pkg/rpc/drpc/chatpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/drpc/greeterpb"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

type greeterServer struct {
	greeterpb.DRPCGreeterUnimplementedServer
}

// Implement the SayHello method
func (g *greeterServer) SayHello(
	ctx context.Context, req *greeterpb.HelloRequest,
) (*greeterpb.HelloResponse, error) {
	log.Printf("Received request: name=%s", req.Name)
	return &greeterpb.HelloResponse{
		Message: fmt.Sprintf("Hello, %s! Pong!", req.Name),
	}, nil
}

type chatServer struct {
	chatpb.DRPCChatServiceUnimplementedServer
}

func (s *chatServer) ChatStream(stream chatpb.DRPCChatService_ChatStreamStream) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("stream closed or errored: %v", err)
			return err
		}

		log.Printf("Received from %s: %s", msg.Sender, msg.Text)

		// Echo back the message with a prefix
		response := &chatpb.ChatMessage{
			Sender: "Server",
			Text:   "Echo: " + msg.Text,
		}

		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func StartServer() error {
	// Listen on TCP
	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	log.Println("Server listening on :9090")

	// create a listen mux that evalutes enough bytes to recognize the DRPC header
	lisMux := drpcmigrate.NewListenMux(lis, len(drpcmigrate.DRPCHeader))
	// Start the mux in a background goroutine
	go func() {
		if err := lisMux.Run(context.Background()); err != nil {
			log.Fatalf("ListenMux run failed: %v", err)
		}
	}()
	// grab the listen mux route for the DRPC Header and default listener
	drpcLis := lisMux.Route(drpcmigrate.DRPCHeader)

	// Create RPC server
	greeter := &greeterServer{}
	m := drpcmux.New()
	// Register the greeter server
	err = greeterpb.DRPCRegisterGreeter(m, greeter)
	if err != nil {
		log.Fatalf("failed to register greeter server: %v", err)
	}
	// Create DRPC server
	s := drpcserver.New(m)
	ctx := context.Background()
	return s.Serve(ctx, drpcLis)
	return nil
}

func StartChatServer() error {
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	// create a listen mux that evalutes enough bytes to recognize the DRPC header
	lisMux := drpcmigrate.NewListenMux(listener, len(drpcmigrate.DRPCHeader))
	// Start the mux in a background goroutine
	go func() {
		if err := lisMux.Run(context.Background()); err != nil {
			log.Fatalf("ListenMux run failed: %v", err)
		}
	}()
	// grab the listen mux route for the DRPC Header and default listener
	drpcLis := lisMux.Route(drpcmigrate.DRPCHeader)

	mux := drpcmux.New()
	if err := chatpb.DRPCRegisterChatService(mux, &chatServer{}); err != nil {
		log.Fatalf("Failed to register: %v", err)
	}
	server := drpcserver.New(mux)

	log.Println("DRPC server listening on :9000")
	//for {
	//	conn, err := listener.Accept()
	//	if err != nil {
	//		log.Printf("Accept error: %v", err)
	//		continue
	//	}
	//	go func(conn net.Conn) {
	//		ctx := context.Background()
	//		if err := server.ServeOne(ctx, conn); err != nil {
	//			log.Printf("Serve error: %v", err)
	//		}
	//	}(conn)
	//}
	//
	// run the server
	// N.B.: if you want TLS, you need to wrap the net.Listener with
	// TLS before passing to Serve here.
	ctx := context.Background()
	return server.Serve(ctx, drpcLis)
	return nil
}
