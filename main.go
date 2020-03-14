package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
)

type VectorClk struct {
	Size   int
	Values []int
}

type Address struct {
	IP   string
	Port string
}

type Message struct {
	Transcript  string
	OID         string
	SenderIndex int
	Timestamp   VectorClk
}

func (m Message) String() string {
	return m.OID + ": " + m.Transcript
}

type ServerReply struct{}

type Server struct {
	ID                string
	Port              string
	IP                string
	Index             int
	Clock             VectorClk
	postponedMessages []Message
	deliveredMessages []Message
}

func (s *Server) deliverMsg(msg Message) {
	s.deliveredMessages = append(s.deliveredMessages, msg)
	fmt.Println(msg)
	fmt.Print("> ")
}

func (s *Server) deliverPostponedMessages() {
	for {
		deliveredIndexes := make(map[int]struct{}, len(s.postponedMessages))
		for i, msg := range s.postponedMessages {
			if s.Clock.ValidateTS(msg.Timestamp, msg.SenderIndex) {
				s.Clock.Update(msg.Timestamp)
				s.deliverMsg(msg)
				deliveredIndexes[i] = struct{}{}
			}
		}
		if len(deliveredIndexes) == 0 {
			break
		}
		for index := range deliveredIndexes {
			l := len(s.postponedMessages)
			s.postponedMessages[index] = s.postponedMessages[l-1]
			s.postponedMessages = s.postponedMessages[:l-1]
		}
	}
}

func (s *Server) MessagePost(msg *Message, reply *ServerReply) error {
	s.postponedMessages = append(s.postponedMessages, *msg)
	// Directly deliver the message for now.
	s.deliverPostponedMessages()
	return nil
}

func setServerID(s *Server, ip string, port string) {
	s.ID = ip + "/" + port
	s.Port = port
	s.IP = ip
}

func setServerClock(s *Server, size int) {
	s.Clock = VectorClk{
		Size:   size,
		Values: make([]int, size),
	}
}

func incrementClock(s *Server) {
	s.Clock.Values[s.Index]++
}

func (clock VectorClk) ValidateTS(ts VectorClk, senderIndex int) bool {
	if ts.Values[senderIndex]-clock.Values[senderIndex] != 1 {
		return false
	}
	for i := 0; i < ts.Size; i++ {
		if i == senderIndex {
			continue
		}
		if ts.Values[i] > clock.Values[i] {
			return false
		}
	}
	return true
}

func (clock *VectorClk) Update(ts VectorClk) {
	for i := 0; i < ts.Size; i++ {
		if ts.Values[i] > clock.Values[i] {
			clock.Values[i] = ts.Values[i]
		}
	}
}

var addresses []Address

func idToAddress(id string) Address {
	addr := strings.Split(id, "/")
	return Address{IP: addr[0], Port: addr[1]}
}

func loadAddresses(server *Server) {
	fmt.Println("Loading addresses...")
	file, err := os.Open("peers.txt")
	defer file.Close()

	if err != nil {
		fmt.Println("Could not open the peers file.")
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	i := -1
	for scanner.Scan() {
		i++
		addr := scanner.Text()
		if addr == server.ID {
			server.Index = i
			continue
		}
		addresses = append(addresses, idToAddress(addr))
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Could not read the peers file.")
		panic(err)
	}
	fmt.Println("Loaded", len(addresses), "addresses.")
}

func runServer(server *Server) {
	// Start the server.
	lst, err := net.Listen("tcp", ":"+server.Port)
	if err != nil {
		fmt.Println("Could not start the server.")
		panic(err)
	}
	for {
		conn, err := lst.Accept()
		if err != nil {
			fmt.Println("Could not accept the connection.")
			fmt.Println(err)
			continue
		}
		rpc.ServeConn(conn)
	}
}

// Finds the IP (v4) of this peer.
// Taken from https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func getSelfIP() string {
	host, _ := os.Hostname()
	addrs, _ := net.LookupIP(host)
	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			return ipv4.String()
		}
	}
	return ""
}

func main() {
	var port string
	if len(os.Args) < 2 {
		fmt.Print("> Port: ")
		port = "8081"
	} else {
		port = os.Args[1]
	}
	ip := getSelfIP()
	// Load the server.
	server := new(Server)
	// Construct the server. Order of these calls are important.
	setServerID(server, ip, port)
	loadAddresses(server)
	setServerClock(server, len(addresses)+1)
	rpc.Register(server)
	// Run the server.
	go runServer(server)
	fmt.Println("Welcome to the chat room.")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		incrementClock(server)
		msg := Message{
			Transcript:  input,
			OID:         server.ID,
			SenderIndex: server.Index,
			Timestamp:   server.Clock,
		}
		// multicast the message
		for _, address := range addresses {
			go func(address Address) {
				client, err := rpc.Dial("tcp", address.IP+":"+address.Port)
				if err != nil {
					fmt.Println("Could not connect to the peer", address.IP+"/"+address.Port)
					return
				}
				defer client.Close()
				client.Call("Server.MessagePost", msg, ServerReply{})
			}(address)
		}
	}
}
