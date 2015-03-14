package zookeeper

import (
	"log"
	"net/url"
	"strconv"
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
	exists, _, err = c.Exists(uri.Path + "/services")
	if err != nil {
		log.Println("zookeeper: error checking if exists:", err)
	}
	if (! exists) {
		log.Println("zookeeper: creating base type path: " + uri.Path + "/services")
		c.Create(uri.Path + "/services", []byte{}, 0, zk.WorldACL(zk.PermAll))
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

func (r *ZkClient) createNode(p string, body []byte) {
	exists, _, _ := r.client.Exists(p)
	if !exists {
		acl := zk.WorldACL(zk.PermAll)
		log.Println("zookeeper: creating path " + p)
		_, err := r.client.Create(p, body, 1, acl)
		if err != nil {
			log.Println("zookeeper: failed to register service:", err)
		}
	}
}

func (r *ZkClient) deleteNodeIfExists(p string) { 
	exists, _, _ := r.client.Exists(p)
	if exists {
		log.Println("zookeeper: deleting path " + p)
		err := r.client.Delete(p, -1)
		if err != nil {
			log.Println("zookeeper: failed to delete container path: ", err)
		}
	}
}
func (r *ZkClient) Register(service *bridge.Service) error {
	// fmt.Printf("Here is service: %+v\n", service)
	privatePort, _ := strconv.Atoi(service.Origin.ExposedPort)
	acl := zk.WorldACL(zk.PermAll)
	exists, _, err := r.client.Exists(r.path + "/" + service.Name)
	if err != nil {
		log.Println("zookeeper: error checking if exists:", err)
	}

	if (! exists) {
		r.client.Create(r.path + "/containers/" + service.Origin.ContainerHostname, []byte{}, 0, acl)
	}

	zbody := &ZnodeBody{Name: service.Name, IP: service.IP, PublicPort: service.Port, PrivatePort: privatePort, Tags: service.Tags, Attrs: service.Attrs}
	body, err  := json.Marshal(zbody)
	if err != nil {
		log.Println("zookeeper: failed to json encode znode body:", err)
	}

	path := r.path + "/containers/" + service.Origin.ContainerHostname + "/" + service.Origin.ExposedPort
	r.client.Create(path, body, 1, acl) // 1 == ephemeral

	for _,tag := range service.Tags {
		basePath := r.path + "/services/" + tag
		r.client.Create(basePath, []byte{}, 0, acl)
		servicePath := basePath + "/" + service.Origin.ExposedPort
		r.client.Create(servicePath, body, 1, acl) // 1 == ephemeral
	}
	return nil
}

func (r *ZkClient) Deregister(service *bridge.Service) error {
	basePath := r.path + "/containers/" + service.Origin.ContainerHostname
	servicePath := basePath + "/" + service.Origin.ExposedPort
	err := r.client.Delete(servicePath, -1)
	if err != nil {
		log.Println("zookeeper: failed to deregister service:", err)
	}

	err = r.client.Delete(servicePath, -1) // -1 means latest version number
	for _,tag := range service.Tags {
		tagPath := r.path + "/services/" + tag + "/" + service.Origin.ExposedPort
		r.deleteNodeIfExists(tagPath)
	}
	children, _, err := r.client.Children(basePath)
	if len(children) == 0 {
		log.Println("zookeeper: deregister empty container path: " + basePath)
		r.deleteNodeIfExists(basePath)
	}
	for _,tag := range service.Tags {
		tagPath := r.path + "/services/" + tag
		tagServicePath := tagPath + "/" + service.Origin.ExposedPort
		children, _, err := r.client.Children(tagServicePath)
		if err != nil {
			log.Println("zookeeper: r.Client.Children failed: ", err)
		}
		if len(children) == 0 {
			log.Println("zookeeper: deregister empty container path: " + tagServicePath)
			r.deleteNodeIfExists(tagServicePath)
		}
		children, _, err = r.client.Children(tagPath)
		if err != nil {
			log.Println("zookeeper: r.Client.Children failed: ", err)
		}
		if len(children) == 0 {
			log.Println("zookeeper: deregister empty container path: " + tagPath)
			r.deleteNodeIfExists(tagPath)
		}
	}

	return err
}

func (r *ZkClient) Refresh(service *bridge.Service) error {
	return r.Register(service)
}
