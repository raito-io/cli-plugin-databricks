//go:build syncintegration

package main

import (
	"context"
	"testing"

	"github.com/aws/smithy-go/ptr"
	"github.com/raito-io/cli/base"
	"github.com/raito-io/cli/base/access_provider"
	"github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/types"
	"github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"cli-plugin-databricks/databricks"
	"cli-plugin-databricks/databricks/it"
)

func TestSync(t *testing.T) {
	ctx := context.Background()
	cfg := it.ReadDatabaseConfig()

	logger = base.Logger()

	testMethod := func(tf func(ctx context.Context, cfg *config.ConfigMap, t *testing.T)) func(t *testing.T) {
		return func(t *testing.T) {
			tf(ctx, cfg, t)
		}
	}

	t.Run("DataSourceSync", testMethod(DataSourceSync))

	t.Run("IdentityStoreSync", testMethod(IdentityStoreSync))

	t.Run("AccessSync", testMethod(AccessSync))

	t.Run("DataUsageSync", testMethod(DataUsageSync))

}

func DataSourceSync(ctx context.Context, cfg *config.ConfigMap, t *testing.T) {
	// Given
	dataSourceSyncer := databricks.NewDataSourceSyncer()

	dsHandler := mocks.NewSimpleDataSourceObjectHandler(t, 1)

	//When
	err := dataSourceSyncer.SyncDataSource(ctx, dsHandler, &data_source.DataSourceSyncConfig{ConfigMap: cfg})

	// Then
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(dsHandler.DataObjects), 200)
}

func IdentityStoreSync(ctx context.Context, cfg *config.ConfigMap, t *testing.T) {
	// Given
	identityStoreSyncer := databricks.NewIdentityStoreSyncer()
	isHandler := mocks.NewSimpleIdentityStoreIdentityHandler(t, 1)

	// When
	err := identityStoreSyncer.SyncIdentityStore(ctx, isHandler, cfg)

	// Then
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(isHandler.Users), 6)
	assert.GreaterOrEqual(t, len(isHandler.Groups), 9)
}

func AccessSync(ctx context.Context, cfg *config.ConfigMap, t *testing.T) {
	accessSyncer := databricks.NewAccessSyncer()

	t.Run("Sync to Target", func(t *testing.T) {
		// Given
		feedbackHandler := mocks.NewSimpleAccessProviderFeedbackHandler(t)

		apImport := sync_to_target.AccessProviderImport{
			AccessProviders: []*sync_to_target.AccessProvider{
				{
					Name:   "Simple AP",
					Id:     "simple-ap-id",
					Action: types.Grant,
					Who: sync_to_target.WhoItem{
						Users:  []string{"c_harris+databricks@raito.io", "5f239a72-c050-47b4-947c-f329f8e2e8f2"},
						Groups: []string{"HUMAN_RESOURCES"},
					},
					What: []sync_to_target.WhatItem{
						{
							DataObject: &data_source.DataObjectReference{
								FullName: "48ec2bd5-3d34-4b42-9784-7a8628c963bf.raito_testing.person.address",
								Type:     "table",
							},
							Permissions: []string{
								"SELECT", "MODIFY",
							},
						},
					},
				},
			},
		}

		// When
		err := accessSyncer.SyncAccessProviderToTarget(ctx, &apImport, feedbackHandler, cfg)

		// Then
		require.NoError(t, err)
		assert.ElementsMatch(t, feedbackHandler.AccessProviderFeedback, []sync_to_target.AccessProviderSyncFeedback{
			{
				AccessProvider: "simple-ap-id",
				ActualName:     "simple-ap-id",
				Type:           ptr.String(access_provider.AclSet),
				Errors:         nil,
				Warnings:       nil,
			},
		})
	})

	t.Run("Sync from target", func(t *testing.T) {
		// Given
		apHandler := mocks.NewSimpleAccessProviderHandler(t, 15)

		// When
		err := accessSyncer.SyncAccessProvidersFromTarget(ctx, apHandler, cfg)

		// Then
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(apHandler.AccessProviders), 10)
	})
}

func DataUsageSync(ctx context.Context, config *config.ConfigMap, t *testing.T) {
	// Given
	dataUsageSyncer := databricks.NewDataUsageSyncer()

	dataUsageHandler := mocks.NewSimpleDataUsageStatementHandler(t)

	// When
	err := dataUsageSyncer.SyncDataUsage(ctx, dataUsageHandler, config)

	// Then
	require.NoError(t, err)
	assert.NotEmpty(t, dataUsageHandler.Statements)
}
