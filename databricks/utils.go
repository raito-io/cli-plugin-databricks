package databricks

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/golang-set/set"
)

var logger hclog.Logger

func init() {
	logger = base.Logger()
}

func cleanDoubleQuotes(input string) string {
	if len(input) >= 2 && strings.HasPrefix(input, "\"") && strings.HasSuffix(input, "\"") {
		return input[1 : len(input)-1]
	}

	return input
}

func getAndValidateParameters(configParams *config.ConfigMap) (accountId string, username string, password string, err error) {
	accountId = configParams.GetString(DatabricksAccountId)

	if accountId == "" {
		return "", "", "", fmt.Errorf("%s is not set", DatabricksAccountId)
	}

	username = configParams.GetString(DatabricksUser)
	if username == "" {
		return "", "", "", fmt.Errorf("%s is not set", DatabricksUser)
	}

	password = configParams.GetString(DatabricksPassword)
	if password == "" {
		return "", "", "", fmt.Errorf("%s is not set", DatabricksPassword)
	}

	return accountId, username, password, nil
}

func addToSetInMap[K comparable, V comparable](m map[K]set.Set[V], k K, v ...V) {
	if _, ok := m[k]; !ok {
		m[k] = set.NewSet[V](v...)
	} else {
		m[k].Add(v...)
	}
}
