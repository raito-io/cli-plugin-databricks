package utils

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/platform"
	repo "cli-plugin-databricks/databricks/repo/types"
)

func GetAndValidateParameters(configParams *config.ConfigMap) (pltfrm platform.DatabricksPlatform, accountId string, repoCredentials repo.RepositoryCredentials, err error) {
	accountId = configParams.GetString(constants.DatabricksAccountId)

	if accountId == "" {
		return 0, "", repo.RepositoryCredentials{}, fmt.Errorf("%s is not set", constants.DatabricksAccountId)
	}

	pltfrm, err = platform.DatabricksPlatformString(strings.ToLower(configParams.GetString(constants.DatabricksPlatform)))
	if err != nil {
		return 0, "", repo.RepositoryCredentials{}, fmt.Errorf("invalid platform: %w", err)
	}

	return pltfrm, accountId, repo.GenerateConfig(configParams), nil
}

func AddToSetInMap[K comparable, V comparable](m map[K]set.Set[V], k K, v ...V) {
	if _, ok := m[k]; !ok {
		m[k] = set.NewSet[V](v...)
	} else {
		m[k].Add(v...)
	}
}

type workspaceRepo interface {
	Ping(ctx context.Context) error
}

func InitializeWorkspaceRepoCredentials(repoCredentials repo.RepositoryCredentials, pltfrm platform.DatabricksPlatform, workspace *provisioning.Workspace) (*repo.RepositoryCredentials, error) {
	if workspace == nil {
		return nil, errors.New("unable to find workspace")
	}

	if pltfrm == platform.DatabricksPlatformAzure && workspace.AzureWorkspaceInfo != nil && repoCredentials.AzureClientId != "" {
		repoCredentials.AzureResourceId = fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Databricks/workspaces/%s", workspace.AzureWorkspaceInfo.SubscriptionId, workspace.AzureWorkspaceInfo.ResourceGroup, workspace.WorkspaceName)
	} else {
		host, err := pltfrm.WorkspaceAddress(workspace.DeploymentName)
		if err != nil {
			return nil, fmt.Errorf("workspace address for workspace %q: %w", workspace.WorkspaceName, err)
		}

		repoCredentials.Host = host
	}

	return &repoCredentials, nil
}

func InitWorkspaceRepo[R workspaceRepo](ctx context.Context, repoCredentials repo.RepositoryCredentials, pltfrm platform.DatabricksPlatform, workspace *provisioning.Workspace, repoFn func(*repo.RepositoryCredentials, int64) (R, error)) (R, error) {
	var r R

	credentials, err := InitializeWorkspaceRepoCredentials(repoCredentials, pltfrm, workspace)
	if err != nil {
		return r, fmt.Errorf("load workspace credentials: %w", err)
	}

	repo, err := repoFn(credentials, workspace.WorkspaceId)
	if err != nil {
		return r, fmt.Errorf("generating repository for %q: %w", workspace.WorkspaceName, err)
	}

	err = repo.Ping(ctx)
	if err != nil {
		return r, fmt.Errorf("pinging workspace %q: %w", workspace.WorkspaceName, err)
	}

	return repo, nil
}
