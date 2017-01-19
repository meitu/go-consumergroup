package consumergroup

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"sort"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// SliceRemoveDuplicates removes duplicate elements from the slice.
func SliceRemoveDuplicates(slice []string) []string {
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

// GenConsumerID generates a consumer ID by host name, current time and
// a random number.
func GenConsumerID() string {
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

func mkdirRecursive(c *zk.Conn, zkPath string) error {
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

func ZKCreateEphemeralPath(c *zk.Conn, zkPath string, data []byte) error {
	return ZKCreateRecursive(c, zkPath, zk.FlagEphemeral, data)
}

func ZKCreatePersistentPath(c *zk.Conn, zkPath string, data []byte) error {
	return ZKCreateRecursive(c, zkPath, 0, data)
}

// ZKCreateRecursive creates the zkPath recursively if it does not exist.
func ZKCreateRecursive(c *zk.Conn, zkPath string, flags int32, data []byte) error {
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

// ZKSetPersistentPath writes data to the node specified by zkPath. zkPath
// will be created recursively if it does not exist in zookeeper.
func ZKSetPersistentPath(c *zk.Conn, zkPath string, data []byte) error {
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
