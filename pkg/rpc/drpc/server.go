package drpc

import (
	"context"
	"fmt"
	"log"
	"net"

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
	// grap the listen mux route for the DRPC Header and default listener
	drpcLis := lisMux.Route(drpcmigrate.DRPCHeader)

	// Create RPC server
	greeter := &greeterServer{}
	m := drpcmux.New()
	//s := drpcserver.New(drpcmux.New())
	// Register the greeter server
	err = greeterpb.DRPCRegisterGreeter(m, greeter)
	if err != nil {
		log.Fatalf("failed to register greeter server: %v", err)
	}
	// Create DRPC server
	s := drpcserver.New(m)
	// Accept loop
	/*for {
		conn, err := lis.Accept()
		log.Printf("accepted connection from %v", conn.RemoteAddr())
		if err != nil {
			log.Printf("failed to accept: %v", err)
			continue
		}
		defer conn.Close()
		go func() {
			ctx := context.Background()
			if err := s.ServeOne(ctx, conn); err != nil {
				log.Printf("connection error: %v", err)
			}
		}()
	}*/
	ctx := context.Background()
	return s.Serve(ctx, drpcLis)
	return nil
}
