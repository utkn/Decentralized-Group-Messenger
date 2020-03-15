package main

import (
	"bufio"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type VectorClk struct {
	Values    []int
	SelfIndex int
}

func (v VectorClk) String() string {
	vals := []string{}
	for _, val := range v.Values {
		vals = append(vals, strconv.Itoa(val))
	}
	return "<" + strings.Join(vals, " ") + ">"
}

func (v VectorClk) Copy() VectorClk {
	vc := VectorClk{}
	vc.SelfIndex = v.SelfIndex
	vc.Values = make([]int, len(v.Values))
	for i := 0; i < len(v.Values); i++ {
		vc.Values[i] = v.Values[i]
	}
	return vc
}

func (v *VectorClk) Update(ts VectorClk) {
	for i := 0; i < len(ts.Values); i++ {
		if ts.Values[i] > v.Values[i] {
			v.Values[i] = ts.Values[i]
		}
	}
	fmt.Println("Updated my clock:", *v)
}

func (v *VectorClk) Increment() {
	v.Values[v.SelfIndex] = v.Values[v.SelfIndex] + 1
	fmt.Println("Incremented my clock:", *v)
}

func (clock VectorClk) ValidateTS(ts VectorClk) bool {
	senderIndex := ts.SelfIndex
	if ts.Values[senderIndex]-clock.Values[senderIndex] != 1 {
		return false
	}
	for i := 0; i < len(ts.Values); i++ {
		if i == senderIndex {
			continue
		}
		if ts.Values[i] > clock.Values[i] {
			return false
		}
	}
	return true
}

type Address struct {
	IP   string
	Port string
}

type Message struct {
	Transcript string
	OID        string
	Timestamp  VectorClk
}

func (m Message) String() string {
	return m.OID + ": " + m.Transcript
}

type ServerReply struct{}

type Server struct {
	ID                string
	Port              string
	IP                string
	Clock             VectorClk
	postponedMessages []Message
	deliveredMessages []Message
}

func (s *Server) deliverMsg(msg Message) {
	s.deliveredMessages = append(s.deliveredMessages, msg)
	fmt.Println(msg)
}

func (s *Server) deliverPostponedMessages() {
	for {
		deliveredIndexes := make(map[int]struct{}, len(s.postponedMessages))
		for i, msg := range s.postponedMessages {
			if s.Clock.ValidateTS(msg.Timestamp) {
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

func (s *Server) printBuffer() {
	vals := []string{}
	fmt.Print("Current buffer: [")
	for _, msg := range s.postponedMessages {
		vals = append(vals, msg.Transcript+msg.Timestamp.String())
	}
	fmt.Print(strings.Join(vals, ", "))
	fmt.Println("]")
}

func (s *Server) MessagePost(msg *Message, reply *ServerReply) error {
	fmt.Println("Received", msg, msg.Timestamp)
	s.postponedMessages = append(s.postponedMessages, *msg)
	s.deliverPostponedMessages()
	s.printBuffer()
	return nil
}

func setServerID(s *Server, ip string, port string) {
	s.ID = ip + "/" + port
	s.Port = port
	s.IP = ip
}

func setServerClock(s *Server, size int) {
	s.Clock.Values = make([]int, size)
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
			server.Clock.SelfIndex = i
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
	fmt.Println("Welcome to the chat room,", server.ID)
	fmt.Printf("Index: %d\n", server.Clock.SelfIndex)
	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		server.Clock.Increment()
		msg := Message{
			Transcript: input,
			OID:        server.ID,
			Timestamp:  server.Clock.Copy(),
		}
		// multicast the message
		for i, address := range addresses {
			go func(delay int, address Address) {
				time.Sleep(time.Duration(delay) * time.Millisecond)
				client, err := rpc.Dial("tcp", address.IP+":"+address.Port)
				if err != nil {
					fmt.Println("Could not connect to the peer", address.IP+"/"+address.Port)
					return
				}
				defer client.Close()
				client.Call("Server.MessagePost", msg, ServerReply{})
			}(i*1000, address)
		}
	}
}
