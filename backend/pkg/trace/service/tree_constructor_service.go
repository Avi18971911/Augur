package service

import (
	"errors"
	"github.com/Avi18971911/Augur/pkg/trace/model"
)

type TreeConstructorService struct {
}

func NewTreeConstructorService() *TreeConstructorService {
	return &TreeConstructorService{}
}

func (tcs *TreeConstructorService) ConstructTree(spans []model.Span) (*TreeNode, error) {
	tree := make(map[string]*TreeNode)
	var root *TreeNode
	for _, span := range spans {
		curNode, ok := tree[span.SpanID]
		if !ok {
			curNode = &TreeNode{Span: span}
			tree[span.SpanID] = curNode
		} else {
			curNode.Span = span
		}

		if span.ParentSpanID != "" {
			parentNode, ok := tree[span.ParentSpanID]
			if !ok {
				parentNode = &TreeNode{}
				tree[span.ParentSpanID] = parentNode
			}
			curNode.Parent = parentNode
			parentNode.Children = append(parentNode.Children, curNode)
		} else {
			root = curNode
		}
	}

	if root == nil {
		return nil, errors.New("root node not found for spans")
	} else {
		return root, nil
	}
}

type TreeNode struct {
	Span     model.Span
	Children []*TreeNode
	Parent   *TreeNode
}
