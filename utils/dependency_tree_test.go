package utils

import (
	"testing"

	"github.com/raito-io/golang-set/set"
	"github.com/stretchr/testify/require"
)

func TestDependencyTree(t *testing.T) {
	dtree := NewDependencyTree[string]()

	err := dtree.AddDependency("AP1")
	require.NoError(t, err)

	err = dtree.AddDependency("AP2", "AP1", "AP3")
	require.NoError(t, err)

	err = dtree.AddDependency("AP3", "AP1")
	require.NoError(t, err)

	err = dtree.DependencyCleanup()
	require.NoError(t, err)

	nodes := make([]string, 0, 3)

	err = dtree.BreadthFirstTraversal(func(n string) error {
		nodes = append(nodes, n)

		return nil
	})
	require.NoError(t, err)

	require.Equal(t, []string{"AP1", "AP3", "AP2"}, nodes)
}

func TestDependencyTree_ErrorOnIntroducingCycle(t *testing.T) {
	dtree := NewDependencyTree[string]()

	err := dtree.AddDependency("AP1")
	require.NoError(t, err)

	err = dtree.AddDependency("AP2", "AP1", "AP3")
	require.NoError(t, err)

	err = dtree.AddDependency("AP3", "AP1", "AP4")
	require.NoError(t, err)

	err = dtree.AddDependency("AP4", "AP2")
	require.Error(t, err)
}

func TestDependencyTree_ComplexImport(t *testing.T) {
	importInput := []struct {
		Name      string
		DependsOn []string
	}{
		{
			Name: "AP1",
			DependsOn: []string{
				"AP2",
				"AP3",
			},
		},
		{
			Name: "AP2",
			DependsOn: []string{
				"AP3",
				"AP4",
			},
		},
		{
			Name:      "AP3",
			DependsOn: []string{},
		},
		{
			Name:      "AP4",
			DependsOn: []string{},
		},
		{
			Name: "AP5",
			DependsOn: []string{
				"AP3",
				"AP6",
			},
		},
		{
			Name:      "AP6",
			DependsOn: []string{"AP1"},
		},
	}

	// Create dependency map to evaluate
	dependencyMap := make(map[string][]string)
	for _, importItem := range importInput {
		dependencyMap[importItem.Name] = importItem.DependsOn
	}

	tree := NewDependencyTree[string]()

	for _, importItem := range importInput {
		err := tree.AddDependency(importItem.Name, importItem.DependsOn...)
		require.NoError(t, err)
	}

	err := tree.DependencyCleanup()
	require.NoError(t, err)

	visitedNodes := set.NewSet[string]()

	// When
	err = tree.BreadthFirstTraversal(func(n string) error {
		dependencies := dependencyMap[n]
		for _, dependency := range dependencies {
			require.Truef(t, visitedNodes.Contains(dependency), "expected node %s to be visited before node %s", dependency, n)
		}

		visitedNodes.Add(n)

		return nil
	})

	require.NoError(t, err)
}

func TestDependencyTree_ComplexImport_MissingAP(t *testing.T) {
	importInput := []struct {
		Name      string
		DependsOn []string
	}{
		{
			Name: "AP1",
			DependsOn: []string{
				"AP2",
				"AP3",
			},
		},
		{
			Name: "AP2",
			DependsOn: []string{
				"AP3",
				"AP4",
			},
		},
		{
			Name:      "AP3",
			DependsOn: []string{},
		},
		// MISSING AP$
		{
			Name: "AP5",
			DependsOn: []string{
				"AP3",
				"AP6",
			},
		},
		{
			Name:      "AP6",
			DependsOn: []string{"AP1"},
		},
	}

	// Create dependency map to evaluate
	dependencyMap := make(map[string][]string)
	for _, importItem := range importInput {
		dependencyMap[importItem.Name] = importItem.DependsOn
	}

	tree := NewDependencyTree[string]()

	for _, importItem := range importInput {
		err := tree.AddDependency(importItem.Name, importItem.DependsOn...)
		require.NoError(t, err)
	}

	err := tree.DependencyCleanup()
	require.Error(t, err)

}
