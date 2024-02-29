package platform

import "errors"

//go:generate go run github.com/raito-io/enumer -type=DatabricksPlatform -trimprefix=DatabricksPlatform -transform=lower
type DatabricksPlatform int

const (
	DatabricksPlatformAzure DatabricksPlatform = iota + 1
	DatabricksPlatformGCP
	DatabricksPlatformAWS
)

func (p DatabricksPlatform) Host() (string, error) {
	switch p {
	case DatabricksPlatformAzure:
		return "https://accounts.azuredatabricks.net", nil
	case DatabricksPlatformGCP:
		return "https://accounts.gcp.databricks.com", nil
	case DatabricksPlatformAWS:
		return "https://accounts.cloud.databricks.com", nil
	default:
		return "", errors.New("unsupported platform")
	}
}
