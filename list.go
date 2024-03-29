package main

type Node struct {
	Val  *Gobj
	next *Node
	pre  *Node
}

type ListType struct {
	EqualFunc func(a, b *Gobj) bool
}

type List struct {
	ListType
	head   *Node
	tail   *Node
	length int
}

func ListCreate(listType ListType) *List {
	var list List
	list.ListType = listType
	return &list
}

func (list *List) Length() int {
	return list.length
}

func (list *List) First() *Node {
	return list.head
}

func (list *List) Last() *Node {
	return list.tail
}

func (list *List) Find(val *Gobj) *Node {
	p := list.head
	for p != nil {
		if list.EqualFunc(p.Val, val) {
			break
		}
		p = p.next
	}
	return p
}

func (list *List) Append(val *Gobj) {
	var n Node
	n.Val = val
	if list.head == nil {
		list.head = &n
		list.tail = &n
	} else {
		n.pre = list.tail
		list.tail.next = &n
		list.tail = list.tail.next
	}
	list.length += 1
}

func (list *List) LPush(val *Gobj) {
	var n Node
	n.Val = val
	if list.head == nil {
		list.head = &n
		list.tail = &n
	} else {
		n.next = list.head
		list.head.pre = &n
		list.head = &n
	}
	list.length += 1
}

func (list *List) DelNode(n *Node) {
	if n == nil {
		return
	}

	if list.head == n {
		if n.next != nil {
			n.next.pre = nil
		}
		list.head = n.next
		n.next = nil
	} else if list.tail == n {
		if n.pre != nil {
			n.pre.next = nil
		}
		list.tail = n.pre
		n.pre = nil
	} else {
		if n.pre != nil {
			n.pre.next = n.next
		}
		if n.next != nil {
			n.next.pre = n.pre
		}

		n.pre = nil
		n.next = nil
	}

	list.length -= 1
}

func (list *List) Delete(val *Gobj) {
	list.DelNode(list.Find(val))
}
