package databricks

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/golang-set/set"

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
	accountId = configParams.GetString(DatabricksAccountId)

	if accountId == "" {
		return 0, "", repo.RepositoryCredentials{}, fmt.Errorf("%s is not set", DatabricksAccountId)
	}

	username := configParams.GetString(DatabricksUser)
	password := configParams.GetString(DatabricksPassword)
	clientId := configParams.GetString(DatabricksClientId)
	clientSecret := configParams.GetString(DatabricksClientSecret)

	pltfrm, err = platform.DatabricksPlatformString(strings.ToLower(configParams.GetString(DatabricksPlatform)))
	if err != nil {
		return 0, "", repo.RepositoryCredentials{}, fmt.Errorf("invalid platform: %w", err)
	}

	return pltfrm, accountId, repo.RepositoryCredentials{Username: username, Password: password, ClientId: clientId, ClientSecret: clientSecret}, nil
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

func selectWorkspaceRepo[R workspaceRepo](ctx context.Context, repoCredentials *repo.RepositoryCredentials, pltfrm platform.DatabricksPlatform, accountId string, workspaces []string, repoFn func(platform.DatabricksPlatform, string, string, *repo.RepositoryCredentials) (R, error)) (R, string, error) {
	var err error

	for _, workspaceName := range workspaces {
		host, werr := pltfrm.WorkspaceAddress(workspaceName)
		if werr != nil {
			err = multierror.Append(err, werr)
			continue
		}

		repo, werr := repoFn(pltfrm, host, accountId, repoCredentials)
		if werr != nil {
			err = multierror.Append(err, werr)
			continue
		}

		werr = repo.Ping(ctx)
		if werr != nil {
			err = multierror.Append(err, werr)
			continue
		}

		return repo, workspaceName, nil
	}

	var r R

	if err == nil {
		return r, "", fmt.Errorf("no workspace found for metastore")
	}

	return r, "", err
}
