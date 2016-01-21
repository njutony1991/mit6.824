package labrpc

//
// channel-based RPC, for 824 labs.
// allows tests to disconnect RPC connections.
//
// we will use the original labrpc.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test against the original before submitting.
//
// adapted from Go net/rpc/server.go.
//
// sends gob-encoded values to ensure that RPCs
// don't include references to program objects.
//
// net := MakeNetwork() -- holds network, clients, servers.
// end := net.MakeEnd(endname) -- create a client end-point, to talk to one server.
// net.AddServer(servername, server) -- adds a named server to network.
// net.Connect(endname, servername) -- connect a client to a server.
// net.Enable(endname, enabled) -- enable/disable a client.
// net.Reliable(bool) -- false means drop/delay messages
//
// end.Call("Raft.AppendEntries", args, &reply) -- send an RPC, wait for reply
//
// srv := MakeServer()
// srv.AddService(svc) -- a server can have multiple services, e.g. Raft and k/v
//   pass srv to net.AddServer()
//
// svc := MakeService(receiverObject) -- obj's methods will handle RPCs
//   much like Go's rpcs.Register()
//   pass svc to srv.AddService()
//

import "encoding/gob"
import "bytes"
import "reflect"
import "fmt"
import "sync"
import "log"
import "strings"
import "math/rand"
import "time"

type reqMsg struct {
	svcMeth  string // e.g. "Raft.AppendEntries"
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type ClientEnd struct {
	ch chan reqMsg
}

// send an RPC, wait for the reply.
// the return value indicates success; false means the
// server couldn't be contacted.
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	e.ch <- req

	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

type Network struct {
	mu          sync.Mutex
	reliable    bool
	longDelays  bool                        // pause a long time on send on disabled connection
	ends        map[interface{}]*ClientEnd  // ends, by name
	enabled     map[interface{}]bool        // by end name
	servers     map[interface{}]*Server     // servers, by name
	connections map[interface{}]interface{} // endname -> servername
}

func MakeNetwork() *Network {
	rn := &Network{}
	rn.reliable = true
	rn.ends = map[interface{}]*ClientEnd{}
	rn.enabled = map[interface{}]bool{}
	rn.servers = map[interface{}]*Server{}
	rn.connections = map[interface{}](interface{}){}
	return rn
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}

// create a client end-point.
// start the thread that listens and delivers.
func (rn *Network) MakeEnd(endname interface{}) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endname)
	}

	e := &ClientEnd{}
	e.ch = make(chan reqMsg)
	rn.ends[endname] = e
	rn.enabled[endname] = false
	rn.connections[endname] = nil

	go func() {
		for xreq := range e.ch {
			// handle the request in a separate thread to avoid
			// blocking subsequent RPCs from the same ClientEnd.
			go func(req reqMsg) {
				rn.mu.Lock()
				enabled := rn.enabled[endname]
				servername := rn.connections[endname]
				var server *Server
				if servername != nil {
					server = rn.servers[servername]
				}
				reliable := rn.reliable
				rn.mu.Unlock()

				if enabled && servername != nil {
					if reliable == false {
						// short delay
						ms := (rand.Int() % 27)
						time.Sleep(time.Duration(ms) * time.Millisecond)
					}

					if reliable == false && (rand.Int()%1000) < 100 {
						// drop the request, return as if timeout
						req.replyCh <- replyMsg{false, nil}
						return
					}

					// execute the request
					reply := server.dispatch(req)

					if reliable == false && (rand.Int()%1000) < 100 {
						// drop the reply, return as if timeout
						req.replyCh <- replyMsg{false, nil}
					} else {
						req.replyCh <- reply
					}
				} else {
					// simulate no reply and eventual timeout.
					ms := 0
					if rn.longDelays {
						// let Raft tests check that leader doesn't send
						// RPCs synchronously.
						ms = (rand.Int() % 7000)
					} else {
						// many kv tests require the client to try each
						// server in fairly rapid succession.
						ms = (rand.Int() % 100)
					}
					time.Sleep(time.Duration(ms) * time.Millisecond)
					req.replyCh <- replyMsg{false, nil}
				}
			}(xreq)
		}
	}()

	return e
}

func (rn *Network) AddServer(servername interface{}, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = rs
}

// connect a ClientEnd to a server.
// a ClientEnd can only be connected once in its lifetime.
func (rn *Network) Connect(endname interface{}, servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.connections[endname] = servername
}

// enable/disable a ClientEnd.
func (rn *Network) Enable(endname interface{}, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.enabled[endname] = enabled
}

// get a server's count of incoming RPCs.
func (rn *Network) GetCount(servername interface{}) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	svr := rn.servers[servername]
	return svr.GetCount()
}

//
// a server is a collection of services, all sharing
// the same rpc dispatcher. so that e.g. both a Raft
// and a k/v server can listen to the same rpc endpoint.
//
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int // incoming RPCs
}

func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()

	rs.count += 1

	// split Raft.AppendEntries into service and method
	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]

	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		log.Fatalf("Server.dispatch(): unknown service %v\n", serviceName)
		return replyMsg{false, nil}
	}
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

// an object with methods that can be called via RPC.
// a single server may have more than one Service.
type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]reflect.Method
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		if method.PkgPath != "" ||
			mtype.NumIn() != 3 ||
			mtype.In(1).Kind() != reflect.Ptr ||
			mtype.In(2).Kind() != reflect.Ptr ||
			mtype.NumOut() != 1 {
			// the method looks like a handler
			svc.methods[mname] = method
		} else {
			// the method is not suitable for a handler
			fmt.Printf("bad method: %v\n", mname)
		}
	}

	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		// prepare space into which to read the argument.
		// the Value's type will be a pointer to req.argsType.
		args := reflect.New(req.argsType)

		// decode the argument.
		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		ad.Decode(args.Interface())

		// allocate space for the reply.
		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		// call the method.
		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		// encode the reply.
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.EncodeValue(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		log.Fatalf("labrpc.Service.dispatch(): no method %v\n", methname)
		return replyMsg{false, nil}
	}
}