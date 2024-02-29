package utils

import (
	"container/list"
	"fmt"
	"io"
	"strings"

	"github.com/raito-io/golang-set/set"
)

type node interface {
	String() string
	Parents() set.Set[node]
	Children() set.Set[node]
	AddChild(node node)
	Level() int
}

type treeNode[T comparable] struct {
	Value    T
	parents  set.Set[node]
	children set.Set[node]
	level    int
}

func newTreeNode[T comparable](value T) *treeNode[T] {
	return &treeNode[T]{
		Value:    value,
		parents:  set.NewSet[node](),
		children: set.NewSet[node](),
		level:    0,
	}
}

func (n *treeNode[T]) String() string {
	return fmt.Sprintf("%v", n.Value)
}

func (n *treeNode[T]) Parents() set.Set[node] {
	return n.parents
}

func (n *treeNode[T]) Children() set.Set[node] {
	return n.children
}

func (n *treeNode[T]) AddChild(node node) {
	n.children.Add(node)
}

func (n *treeNode[T]) Level() int {
	return n.level
}

type rootNode struct {
	children set.Set[node]
}

func (n *rootNode) String() string {
	return "root"
}

func (n *rootNode) Children() set.Set[node] {
	return n.children
}

func (n *rootNode) AddChild(node node) {
	n.children.Add(node)
}

func (n *rootNode) Parents() set.Set[node] {
	return nil
}

func (n *rootNode) Level() int {
	return 0
}

type DependencyTree[T comparable] struct {
	root      rootNode
	nodes     map[T]*treeNode[T]
	freeNodes map[T]*treeNode[T]
}

func NewDependencyTree[T comparable]() *DependencyTree[T] {
	return &DependencyTree[T]{
		root: rootNode{
			children: set.NewSet[node](),
		},
		nodes:     make(map[T]*treeNode[T]),
		freeNodes: make(map[T]*treeNode[T]),
	}
}

func (d *DependencyTree[T]) AddDependency(id T, dependsOn ...T) error {
	if _, ok := d.nodes[id]; ok {
		return fmt.Errorf("id %v already exists", id)
	}

	newNode, isFreeNode := d.freeNodes[id]
	if !isFreeNode {
		newNode = newTreeNode(id)
	} else {
		// Check for cycles
		dependsOnSet := set.NewSet(dependsOn...)

		hasCycle := false

		d.depthFirstTraversal(func(n node, depth int) bool {
			nt := n.(*treeNode[T])
			if dependsOnSet.Contains(nt.Value) {
				hasCycle = true
				return true
			}

			return false
		}, newNode, 0)

		if hasCycle {
			return fmt.Errorf("cycle detected")
		}

		delete(d.freeNodes, id)
	}

	d.nodes[id] = newNode

	if len(dependsOn) == 0 {
		d.root.children.Add(newNode)
		newNode.parents.Add(&d.root)
		newNode.level = 1
	}

	for _, dep := range dependsOn {
		depNode, depFound := d.getNode(dep)

		if !depFound {
			newFreeNode := newTreeNode(dep)

			d.freeNodes[dep] = newFreeNode

			depNode = newFreeNode
		}

		newNode.parents.Add(depNode)
		depNode.children.Add(newNode)
	}

	maxLevel := 0
	for parent := range newNode.parents {
		if parent.Level() >= maxLevel {
			maxLevel = parent.Level() + 1
		}
	}

	newNode.level = maxLevel
	d.updateChildLevel(newNode)

	return nil
}

func (d *DependencyTree[T]) updateChildLevel(n *treeNode[T]) {
	for child := range n.children {
		childTree := child.(*treeNode[T])
		if child.Level() <= n.level {
			childTree.level = n.level + 1
			d.updateChildLevel(childTree)
		}
	}
}

func (d *DependencyTree[T]) DependencyCleanup() error {
	if len(d.freeNodes) != 0 {
		return fmt.Errorf("not all depended nodes have been created")
	}

	d.DepthFirstTraversal(func(n node, depth int) bool {
		if n.Level() == 0 {
			return false
		}

		tNode := n.(*treeNode[T])
		for child := range tNode.children {
			if child.Level() > tNode.level+1 {
				tNode.children.Remove(child)
			}
		}

		return false
	})

	return nil
}

// PrintTree prints the dependency tree for debug reasons
func (d *DependencyTree[T]) PrintTree(writer io.Writer) (n int, err error) {
	var stringbuilder strings.Builder

	d.DepthFirstTraversal(func(n node, depth int) bool {
		for i := 0; i < depth; i++ {
			stringbuilder.WriteString("+")
		}

		stringbuilder.WriteString(n.String())
		stringbuilder.WriteString(fmt.Sprintf(" - %d", n.Level()))

		stringbuilder.WriteString("\n")

		return false
	})

	return writer.Write([]byte(stringbuilder.String()))
}

func (d *DependencyTree[T]) DepthFirstTraversal(fn func(n node, depth int) bool) {
	d.depthFirstTraversal(fn, &d.root, 0)
}

func (d *DependencyTree[T]) depthFirstTraversal(fn func(n node, depth int) bool, n node, depth int) {
	earlyStopping := fn(n, depth)
	if earlyStopping {
		return
	}

	for child := range n.Children() {
		d.depthFirstTraversal(fn, child, depth+1)
	}
}

func (d *DependencyTree[T]) BreadthFirstTraversal(fn func(n T) error) error {
	queue := list.New()

	for child := range d.root.children {
		queue.PushBack(child)
	}

	done := set.NewSet[T]()

	for queue.Len() > 0 {
		queueElement := queue.Front()
		queue.Remove(queueElement)

		n := queueElement.Value.(*treeNode[T])

		err := fn(n.Value)
		if err != nil {
			return err
		}

		for child := range n.Children() {
			childtn := child.(*treeNode[T])
			if done.Contains(childtn.Value) {
				continue
			}

			done.Add(childtn.Value)
			queue.PushBack(child)
		}
	}

	return nil
}

func (d *DependencyTree[T]) getNode(id T) (*treeNode[T], bool) {
	if n, ok := d.nodes[id]; ok {
		return n, true
	}

	if n, ok := d.freeNodes[id]; ok {
		return n, true
	}

	return nil, false
}
