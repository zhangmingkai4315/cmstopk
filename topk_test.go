package cmstopk

import (
	"fmt"
	"testing"
	"time"
)

func TestNewTopkManager(t *testing.T) {
	manager := NewTopkManager(10, time.Millisecond*500,true)
	if manager.heap == nil  && manager.heap.Max != 10{
		t.Error("manager heap should be init success")
	}

	if manager.cms == nil  && manager.cms.d > 0 && manager.cms.w > 0{
		t.Error("manager cms should be init success")
	}
	for i:=0;i<100;i++{
		for j:=0;j<i*10;j++{
			manager.Receive(fmt.Sprintf("baidu%d.com",i),1)
		}
	}
	result := <-manager.topkChan
	if len(result) != 10{
		t.Errorf("expect receive top10 but got len=%d",len(result))
	}

	for i, item := range result{
		expect := fmt.Sprintf("baidu9%d.com:9%d0", i,i)
		if item.String() != expect{
			t.Errorf("expect %s but got %s", expect, item.String())
		}
	}

	for i:=0;i<100;i++{
		for j:=0;j<i*10;j++{
			manager.Receive(fmt.Sprintf("baidu%d.com",i),1)
		}
	}

	result = <-manager.topkChan
	if len(result) != 10{
		t.Errorf("expect receive top10 but got len=%d",len(result))
	}

	for i, item := range result{
		expect := fmt.Sprintf("baidu9%d.com:9%d0", i,i)
		if item.String() != expect{
			t.Errorf("expect %s but got %s", expect, item.String())
		}
	}

}
