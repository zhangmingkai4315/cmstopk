package cmstopk

import "time"

type TopKManager struct {
	TopKChan chan []*ItemNode
	dataChan chan *ItemNode
	reset    bool
	duration time.Duration
	cms      *CountMinSketch
	heap     *Heap
}

func (manager *TopKManager) Receive(item string, count uint64){
	manager.dataChan<-NewItemNode(item, count)
}
func (manager *TopKManager) update(item string, count uint64) {
	v := manager.cms.UpdateString(item, count)
	i, node := manager.heap.Find(item)
	if i == -1 {
		manager.heap.Push(NewItemNode(item, v))
	} else {
		node.Counter = v
		manager.heap.Fix(i)
	}
}

func NewTopkManager(k int, duration time.Duration,reset bool) *TopKManager {
	cms, _ := NewCountMinSketch(5, 10000)
	manager := &TopKManager{
		TopKChan: make(chan []*ItemNode),
		dataChan: make(chan *ItemNode, 10000),
		cms:      cms,
		reset:    reset,
		heap:     NewHeap(k),
	}

	go func() {
		timer := time.NewTicker(duration)
		for {
			select {
			case <-timer.C:
				result := make([]*ItemNode, 0)
				for {
					if item := manager.heap.Pop(); item != nil {
						result = append(result, item)
					} else {
						break
					}
				}
				manager.TopKChan <- result
				if manager.reset == true{
					manager.Reset()
				}
			case item := <-manager.dataChan:
				manager.update(item.Key, item.Counter)
			}
		}
	}()
	return manager
}


func (manager *TopKManager)Reset(){
	manager.cms.Reset()
	manager.heap.Reset()
}