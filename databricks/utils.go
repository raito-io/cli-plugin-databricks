package databricks

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/provisioning"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/golang-set/set"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
)

var logger hclog.Logger

func init() {
	logger = base.Logger()
}

func cleanDoubleQuotes(input string) string { //nolint:unused
	if len(input) >= 2 && strings.HasPrefix(input, "\"") && strings.HasSuffix(input, "\"") {
		return input[1 : len(input)-1]
	}

	return input
}

func getAndValidateParameters(configParams *config.ConfigMap) (pltfrm platform.DatabricksPlatform, accountId string, repoCredentials repo.RepositoryCredentials, err error) {
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

func addToSetInMap[K comparable, V comparable](m map[K]set.Set[V], k K, v ...V) {
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
		return nil, errors.New("Unable to find workspace")
	}
	if pltfrm == platform.DatabricksPlatformAzure && workspace.AzureWorkspaceInfo != nil {
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

func selectWorkspaceRepo[R workspaceRepo](ctx context.Context, repoCredentials repo.RepositoryCredentials, pltfrm platform.DatabricksPlatform, workspaces []*provisioning.Workspace, repoFn func(*repo.RepositoryCredentials) (R, error)) (R, *provisioning.Workspace, error) {
	var err error

	for _, workspace := range workspaces {
		credentials, werr := InitializeWorkspaceRepoCredentials(repoCredentials, pltfrm, workspace)
		if werr != nil {
			err = multierror.Append(err, werr)
			continue
		}

		repo, werr := repoFn(credentials)
		if werr != nil {
			err = multierror.Append(err, fmt.Errorf("generating repository for %q: %w", workspace.WorkspaceName, werr))
			continue
		}

		werr = repo.Ping(ctx)
		if werr != nil {
			err = multierror.Append(err, fmt.Errorf("ping %q: %w", workspace.WorkspaceName, werr))
			continue
		}

		return repo, workspace, nil
	}

	var r R

	if err == nil {
		return r, nil, fmt.Errorf("no workspace found for metastore")
	}

	return r, nil, fmt.Errorf("select workspace: %w", err)
}
