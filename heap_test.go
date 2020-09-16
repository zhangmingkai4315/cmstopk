package cmstopk

import (
	"fmt"
	"testing"
)

func TestHeap(t *testing.T) {
	heap := NewHeap(10)
	strSlice := []string{"abc.com", "index.com", "amazon.com", "baidu.com", "ali.com","fly.com"}
	values := []uint64{10,100,20,23,21,44}
	heap.BuildFromStringSlice(strSlice, values)

	expectValues := []uint64{100,44,23,21,20,10}
	for _ , ev := range expectValues{
		rv := heap.Pop()
		if rv == nil {
			t.Error("expect value != nil ")
		}
		if  rv.Counter != ev{
			t.Errorf("expect value = %d, got %d", ev, rv.Counter)
		}
	}
}


func TestHeapPush(t *testing.T) {
	heap := NewHeap(10)
	for i := 0;i<100;i++{
		heap.Push(NewItemNode(fmt.Sprintf("%d.com",i), uint64(i)))
	}

	expectValues := []uint64{90,91,92,93,94,95,96,97,98,99}
	for _ , ev := range expectValues{
		rv := heap.Pop()
		if rv == nil {
			t.Error("expect value != nil ")
		}
		if  rv.Counter != ev{
			t.Errorf("expect value = %d, got %d", ev, rv.Counter)
		}
	}
}
