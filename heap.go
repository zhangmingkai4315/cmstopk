package cmstopk

import "fmt"

// ItemNode define the heap node
type ItemNode struct {
	Key     string
	Counter uint64
}

// Value return the node value
func (node *ItemNode) String() string {
	return fmt.Sprintf("%s:%d", node.Key, node.Counter)
}

// NewItemNode create a new heap node
func NewItemNode(value string, counter uint64) *ItemNode {
	return &ItemNode{
		Key:     value,
		Counter: counter,
	}
}

//Heap define a MinHeap struct with
//size and structure
type Heap struct {
	Data    []*ItemNode
	Counter int
	Max     int
}

// NewHeap create Heap object
func NewHeap(maxN int) *Heap {
	return &Heap{
		Data:    []*ItemNode{},
		Counter: 0,
		Max:     maxN,
	}
}

// IsEmpty returns true if this priority heap is empty.
func (heap *Heap) IsEmpty() bool {
	return heap.Counter == 0
}

func (heap *Heap) Reset() {
	heap.Counter = 0
	heap.Data = make([]*ItemNode,0)
}


// IsFull returns true if this priority heap is full.
func (heap *Heap) IsFull() bool {
	return heap.Counter == heap.Max
}

// Min return the min value of Data
func (heap *Heap) Min() (*ItemNode, error) {
	if heap.IsEmpty() {
		return nil, nil
	}
	return heap.Data[0], nil
}

func (heap *Heap) BuildFromNodeSlice(data []*ItemNode) {
	n := len(data)
	heap.Data = data
	for i := n/2 - 1; i >= 0; i-- {
		heap.down(i, n)
	}
}

func (heap *Heap) BuildFromStringSlice(data []string, values []uint64) {
	n := len(data)
	nodeSlice := make([]*ItemNode, 0)
	for i, str := range data{
		nodeSlice = append(nodeSlice, NewItemNode(str, values[i]))
	}
	heap.Data = nodeSlice
	heap.Counter = n
	for i := n/2 - 1; i >= 0; i-- {
		heap.down(i, n)
	}
}

func (heap *Heap) Find(key string) (int, *ItemNode) {
	for i, item := range heap.Data {
		if item.Key == key {
			return i, item
		}
	}
	return -1, nil
}
func (heap *Heap) down(index int, size int) bool {
	i := index
	for {
		j1 := 2*i + 1
		if j1 >= size || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < size && heap.Data[j2].Counter < heap.Data[j1].Counter {
			j = j2 // = 2*i + 2
		}
		if !(heap.Data[j].Counter < heap.Data[i].Counter) {
			break
		}
		heap.Data[i], heap.Data[j] = heap.Data[j], heap.Data[i]
		i = j
	}
	return i > index
}

func (heap *Heap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !(heap.Data[j].Counter < heap.Data[i].Counter) {
			break
		}
		heap.Data[i], heap.Data[j] = heap.Data[j], heap.Data[i]
		j = i
	}
}

// Push new element to heap
func (heap *Heap) Push(node *ItemNode) {
	if heap.Counter < heap.Max {
		heap.Counter++
		heap.Data = append(heap.Data, node)
		heap.up(heap.Counter - 1)
	}else{
		if node.Counter < heap.Data[0].Counter{
			return
		}else{
			heap.Data[0] = node
			heap.Fix(0)
		}
	}
}

// Pop the min element from the heap
func (heap *Heap) Pop() *ItemNode {
	if heap.Counter == 0 {
		return nil
	}
	n := heap.Counter - 1
	heap.Data[0], heap.Data[n] = heap.Data[n], heap.Data[0]
	heap.down(0, n)
	heap.Counter--
	return heap.Data[n]
}

// Fix the heap when the element changed
func (heap *Heap) Fix(i int) {
	if !(heap.down(i, heap.Counter)) {
		heap.up(i)
	}
}
