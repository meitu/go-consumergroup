package consumergroup

import (
	"path"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/meitu/zk_wrapper"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	testPath = "/test1/test2/test3/test4"
)

func TestUtilSliceRemoveDuplicates(t *testing.T) {
	slice := []string{"hello", "world", "i", "have", "i", "i", "i", "world", "have"}
	slice = sliceRemoveDuplicates(slice)
	s := []string{"have", "i", "world", "hello"}
	sort.Strings(s)
	sort.Strings(slice)
	if !reflect.DeepEqual(s, slice) {
		t.Error("slice remove duplicates failed")
	}
}

func deleteRecursive(c *zk_wrapper.Conn, zkPath string) error {
	p := zkPath
	for p != "/" {
		err := c.Delete(p, -1)
		if err != nil {
			return err
		}
		p = path.Dir(p)
	}
	return nil
}

func TestUtilMkdirRecursive(t *testing.T) {
	client, _, err := zk_wrapper.Connect([]string{"127.0.0.1:2181"}, time.Duration(6)*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = mkdirRecursive(client, testPath)
	if err != nil {
		t.Fatal(err)
	}

	isExist, _, err := client.Exists(testPath)
	if err != nil {
		t.Fatal(err)
	}
	if !isExist {
		t.Fatal("make directory recursive failed, this path is not exist")
	}

	err = mkdirRecursive(client, testPath)
	if err != nil {
		if err == zk.ErrNodeExists {
			t.Error("expected function mkdirRecursive can ignore make directory repeatedly, but it didn't")
		} else {
			t.Error(err)
		}
	}

	err = deleteRecursive(client, testPath)
	if err != nil {
		t.Error("detele directory recursive failed, please delete by zk client")
	}
}
