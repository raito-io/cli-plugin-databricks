package platform

import (
	"errors"
	"fmt"
)

//go:generate go run github.com/raito-io/enumer -type=DatabricksPlatform -trimprefix=DatabricksPlatform -transform=lower
type DatabricksPlatform int

const (
	DatabricksPlatformAzure DatabricksPlatform = iota + 1
	DatabricksPlatformGCP
	DatabricksPlatformAWS
)

func (p DatabricksPlatform) Host() (string, error) {
	return p.WorkspaceAddress("accounts")
}

func (p DatabricksPlatform) WorkspaceAddress(deploymentId string) (string, error) {
	url, err := p.fmtUrl()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(url, deploymentId), nil
}

func (p DatabricksPlatform) fmtUrl() (string, error) {
	switch p {
	case DatabricksPlatformAzure:
		return "https://%s.azuredatabricks.net", nil
	case DatabricksPlatformGCP:
		return "https://%s.gcp.databricks.com", nil
	case DatabricksPlatformAWS:
		return "https://%s.cloud.databricks.com", nil
	default:
		return "", errors.New("unsupported platform")
	}
}
