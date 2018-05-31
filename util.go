package consumergroup

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"sort"
	"time"

	"github.com/meitu/zk_wrapper"
	"github.com/samuel/go-zookeeper/zk"
)

func sliceRemoveDuplicates(slice []string) []string {
	sort.Strings(slice)
	i := 0
	var j int
	for {
		if i >= len(slice)-1 {
			break
		}
		for j = i + 1; j < len(slice) && slice[i] == slice[j]; j++ {
		}
		slice = append(slice[:i+1], slice[j:]...)
		i++
	}
	return slice
}

func genConsumerID() string {
	name, err := os.Hostname()
	if err != nil {
		name = "unknown"
	}
	currentMilliSec := time.Now().UnixNano() / int64(time.Millisecond)
	randBytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		randBytes[i] = byte(rand.Intn(26) + 65)
	}
	return fmt.Sprintf("%s-%d-%s", name, currentMilliSec, string(randBytes))
}

func mkdirRecursive(c *zk_wrapper.Conn, zkPath string) error {
	var err error
	parent := path.Dir(zkPath)
	if parent != "/" {
		if err = mkdirRecursive(c, parent); err != nil {
			return err
		}
	}
	_, err = c.Create(zkPath, nil, 0, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		err = nil
	}
	return err
}

func zkCreateEphemeralPath(c *zk_wrapper.Conn, zkPath string, data []byte) error {
	return zkCreateRecursive(c, zkPath, zk.FlagEphemeral, data)
}

func zkCreatePersistentPath(c *zk_wrapper.Conn, zkPath string, data []byte) error {
	return zkCreateRecursive(c, zkPath, 0, data)
}

func zkCreateRecursive(c *zk_wrapper.Conn, zkPath string, flags int32, data []byte) error {
	_, err := c.Create(zkPath, data, flags, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	if err == zk.ErrNoNode {
		mkdirRecursive(c, path.Dir(zkPath))
		_, err = c.Create(zkPath, data, flags, zk.WorldACL(zk.PermAll))
	}
	return err
}

func zkSetPersistentPath(c *zk_wrapper.Conn, zkPath string, data []byte) error {
	_, err := c.Set(zkPath, data, -1)
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	if err == zk.ErrNoNode {
		mkdirRecursive(c, path.Dir(zkPath))
		_, err = c.Create(zkPath, data, 0, zk.WorldACL(zk.PermAll))
	}
	return err
}
