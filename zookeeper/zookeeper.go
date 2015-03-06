package zookeeper

import (
	"log"
	"net"
	"net/url"
	"strconv"
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/gliderlabs/registrator/bridge"
)

func init() {
	bridge.Register(new(Factory), "zookeeper")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {

	c, _, err := zk.Connect([]string{uri.Host}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	return &ZkClient{client: c, path: uri.Path}
}

type ZkClient struct {
	client *zk.Conn
	path   string
}

func (r *ZkClient) Ping() error {
	// rr := etcd.NewRawRequest("GET", "version", nil, nil)
	// _, err := r.client.SendRequest(rr)
	// if err != nil {
	// 	return err
	// }
	return nil
}

func (r *ZkClient) Register(service *bridge.Service) error {
	path := r.path + "/" + service.Name + "/" + service.ID
	port := strconv.Itoa(service.Port)
	addr := net.JoinHostPort(service.IP, port)
	acl := zk.WorldACL(zk.PermAll)
	// 1 is FlagEphemeral
	// 2 is FlagSequence
	fmt.Println("\n\npath: " + path + "\n\n")
	fmt.Printf("service: %+v\n", service)
	_, err := r.client.Create(path, []byte(addr), 1, acl)
	if err != nil {
		log.Println("zookeeper: failed to register service:", err)
	}
	return err
}

func (r *ZkClient) Deregister(service *bridge.Service) error {
	path := r.path + "/" + service.Name + "/" + service.ID
	err := r.client.Delete(path, -1) // -1 means latest version number
	if err != nil {
		log.Println("zookeeper: failed to deregister service:", err)
	}
	return err
}

func (r *ZkClient) Refresh(service *bridge.Service) error {
	return r.Register(service)
}
