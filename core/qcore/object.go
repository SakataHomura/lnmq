package qcore

type PriorityObject struct {
    Priority int64
    Index int
    Value interface{}
}

type PriorityQueue []*PriorityObject

func NewPriorityQueue(capacity int) PriorityQueue {
    return make(PriorityQueue, 0, capacity)
}

func (q *PriorityQueue) Len() int {
    return len(*q)
}

func (q *PriorityQueue) Less(i, j int) bool {
    return (*q)[i].Priority < (*q)[j].Priority
}

func (q *PriorityQueue) Swap(i, j int)  {
    (*q)[i], (*q)[j] = (*q)[j], (*q)[i]
    (*q)[i].Index = i
    (*q)[j].Index = j
}

func (q *PriorityQueue) Push(v interface{}) {
    /*
    n := len(*q)
    c := cap(*q)
    if n+1 > c {
        nq := make(PriorityQueue, n, c * 2)
        copy(nq, *q)
        *q = nq
    }

    *q = (*q)[0:n+1]
    item := v.(*Object)
    item.Index = n
    (*q)[n] = item
    */
    o := v.(*PriorityObject)
    o.Index = q.Len()
    *q = append(*q, o)
}

func (q *PriorityQueue) Pop() interface{} {
    n := q.Len()
    o := (*q)[n-1]
    o.Index = -1
    *q = (*q)[:n-1]

    return o
}
/*
func (q *PriorityQueue) Remove()  {

}
*/

func (q *PriorityQueue) PeekAndShift(max int64) (*PriorityObject, int64) {
    n := q.Len()

    if n == 0 {
        return nil, 0
    }

    o := (*q)[0]
    if o.Priority > max {
        return nil, o.Priority - max
    }

    *q = (*q)[1:n]

    return o, 0
}

/*
func (q *PriorityQueue) up(j int)  {
    for {
        i := (j - 1) / 2
        if i ==j || (*q)[j].Priority >= (*q)[i].Priority {
            break
        }
        q.Swap(i, j)
        j = i
    }
}

func (q *PriorityQueue) down(i, n int)  {
    for {
        j1 := i * 2 + 1
        if j1 >= n || j1 < 0 {
            break
        }

        j := j1
        if j2 := j1+1; j2 < n && (*q)[j1].Priority >= (*q)[j2].Priority {
            j = j2
        }

        if (*q)[j].Priority >= (*q)[i].Priority {
            break
        }

        q.Swap(i, j)
        i = j
    }
}
*/