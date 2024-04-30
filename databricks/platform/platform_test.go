package platform

import "testing"

func TestDatabricksPlatform_Host(t *testing.T) {
	tests := []struct {
		name    string
		p       DatabricksPlatform
		want    string
		wantErr bool
	}{
		{
			name:    "Azure",
			p:       DatabricksPlatformAzure,
			want:    "https://accounts.azuredatabricks.net",
			wantErr: false,
		},
		{
			name:    "GCP",
			p:       DatabricksPlatformGCP,
			want:    "https://accounts.gcp.databricks.com",
			wantErr: false,
		},
		{
			name:    "AWS",
			p:       DatabricksPlatformAWS,
			want:    "https://accounts.cloud.databricks.com",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.p.Host()
			if (err != nil) != tt.wantErr {
				t.Errorf("Host() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Host() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatabricksPlatform_WorkspaceAddress(t *testing.T) {
	tests := []struct {
		name         string
		p            DatabricksPlatform
		deploymentId string
		want         string
		wantErr      bool
	}{
		{
			name:         "Azure",
			p:            DatabricksPlatformAzure,
			deploymentId: "deploymentId1",
			want:         "https://deploymentId1.azuredatabricks.net",
			wantErr:      false,
		},
		{
			name:         "GCP",
			p:            DatabricksPlatformGCP,
			deploymentId: "deploymentId2",
			want:         "https://deploymentId2.gcp.databricks.com",
			wantErr:      false,
		},
		{
			name:         "AWS",
			p:            DatabricksPlatformAWS,
			deploymentId: "deploymentId3",
			want:         "https://deploymentId3.cloud.databricks.com",
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.p.WorkspaceAddress(tt.deploymentId)
			if (err != nil) != tt.wantErr {
				t.Errorf("WorkspaceAddress(%q) error = %v, wantErr %v", tt.deploymentId, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WorkspaceAddress(%q) got = %v, want %v", tt.deploymentId, got, tt.want)
			}
		})
	}
}
