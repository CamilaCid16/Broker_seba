package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"

	pb "github.com/yojeje/lab6"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedBrokerServer
	pb.UnimplementedIngenierosServer
	pb.UnimplementedKaisServer
}

var fulcrumServers = []string{
	"dist014:50056",
	"dist015:50057",
	"dist016:50058",
}

func (s *server) EnviarBroker(ctx context.Context, in *pb.Comando) (*pb.Direccion, error) {
	fmt.Println("Comando recibido: " + in.Tipo)
	address := fulcrumServers[rand.Intn(len(fulcrumServers))]
	fmt.Printf("Redirigiendo Ingeniero a %v\n", address)
	return &pb.Direccion{Dir: address}, nil
}

func (s *server) GetEnemigosBroker(ctx context.Context, in *pb.Informacion) (*pb.Direccion, error) {
	fmt.Println("Comando recibido: " + in.Tipo)
	address := fulcrumServers[rand.Intn(len(fulcrumServers))]
	fmt.Printf("Redirigiendo Comandante a %v\n", address)

	return &pb.Direccion{Dir: address}, nil
}

func (s *server) ResolverConsistencia(ctx context.Context, in *pb.Informacion) (*pb.Direccion, error) {
	// Resolver conflicto basado en el vector clock
	// Por simplicidad, asumimos que elegimos un servidor al azar aquí
	address := fulcrumServers[rand.Intn(len(fulcrumServers))]
	fmt.Printf("Resolviendo conflicto para sector %v, base %v, redirigiendo a %v\n", in.GetSector(), in.GetBase(), address)

	return &pb.Direccion{Dir: address}, nil
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 50051))
	if err != nil {
		log.Fatalf("Error to listen: %v", err)
	}
	s :=  grpc.NewServer()
	pb.RegisterIngenierosServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Falla de servidor: %v", err)
	}
}