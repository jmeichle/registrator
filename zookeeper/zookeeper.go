package zookeeper

import (
	"log"
	"net/url"
	"strconv"
	"fmt"
	"time"
	"encoding/json"

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
	exists, _, err := c.Exists(uri.Path)
	if err != nil {
		log.Println("zookeeper: error checking if exists:", err)
	}
	if (! exists) {
		log.Println("zookeeper: creating base path: " + uri.Path)
		c.Create(uri.Path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	}
	exists, _, err = c.Exists(uri.Path + "/containers")
	if err != nil {
		log.Println("zookeeper: error checking if exists:", err)
	}
	if (! exists) {
		log.Println("zookeeper: creating base type path: " + uri.Path + "/containers")
		c.Create(uri.Path + "/containers", []byte{}, 0, zk.WorldACL(zk.PermAll))
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

type ZnodeBody struct {
	Name string
	IP string
	PublicPort int
	PrivatePort int
	Tags []string
	Attrs map[string]string
}

func (r *ZkClient) Register(service *bridge.Service) error {
	fmt.Printf("Here is service: %+v\n", service)
	privatePort, _ := strconv.Atoi(service.Origin.ExposedPort)
	acl := zk.WorldACL(zk.PermAll)
	exists, _, err := r.client.Exists(r.path + "/" + service.Name)
	if err != nil {
		log.Println("zookeeper: error checking if exists:", err)
	}

	if (! exists) {
		log.Println("zookeeper: creating service base path: " + r.path + "/" + service.Name)
		r.client.Create(r.path + "/containers/" + service.Origin.ContainerHostname, []byte{}, 0, acl)
	}

	zbody := &ZnodeBody{Name: service.Name, IP: service.IP, PublicPort: service.Port, PrivatePort: privatePort, Tags: service.Tags, Attrs: service.Attrs}
	body, err  := json.Marshal(zbody)
	if err != nil {
		log.Println("zookeeper: failed to json encode znode body:", err)
	}
	path := r.path + "/containers/" + service.Origin.ContainerHostname + "/" + service.Origin.ExposedPort
	_, err = r.client.Create(path, body, 1, acl)
	if err != nil {
		log.Println("zookeeper: failed to register service:", err)
	}
	return err
}

func (r *ZkClient) Deregister(service *bridge.Service) error {
	path := r.path + "/containers/" + service.Origin.ContainerHostname + "/" + service.Origin.ExposedPort
	log.Println("degregister path: " + path)
	err := r.client.Delete(path, -1) // -1 means latest version number
	if err != nil {
		log.Println("zookeeper: failed to deregister service:", err)
	}
	children, _, err := conn.Children(r.path + "/containers/" + service.Origin.ContainerHostname)
	return err
}

func (r *ZkClient) Refresh(service *bridge.Service) error {
	return r.Register(service)
}
