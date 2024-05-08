package databricks

import (
	"context"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"cli-plugin-databricks/databricks/constants"
	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
	repo2 "cli-plugin-databricks/databricks/repo/types"
)

func TestIdentityStoreSyncer_SyncIdentityStore(t *testing.T) {
	// Given
	service, mockRepo := createIdentityStoreSyncer(t)
	identityHandlerMock := mocks.NewSimpleIdentityStoreIdentityHandler(t, 1)

	mockRepo.EXPECT().ListGroups(mock.Anything).Return(repo.ArrayToChannel([]iam.Group{
		{
			Id:          "gid2",
			DisplayName: "group-2",
			Members: []iam.ComplexValue{
				{
					Value:   "idUser2",
					Display: "user2",
					Ref:     "Users/idUser2",
				},
				{
					Value:   "gid1",
					Display: "group-1",
					Ref:     "Groups/gid1",
				},
				{
					Value:   "ServicePrincipalId1",
					Display: "servicePrincipal-1",
					Ref:     "ServicePrincipal/ServicePrincipalId1",
				},
			},
		},
		{
			Id:          "gid1",
			DisplayName: "group-1",
			Members: []iam.ComplexValue{{
				Value:   "idUser1",
				Display: "user1",
				Ref:     "Users/idUser1",
			}},
		},
	})).Once()

	mockRepo.EXPECT().ListUsers(mock.Anything).Return(repo.ArrayToChannel([]iam.User{
		{
			DisplayName: "user1",
			Id:          "idUser1",
			UserName:    "username1",
			Emails: []iam.ComplexValue{
				{
					Type:    "private",
					Value:   "user1@private.com",
					Primary: false,
				},
				{
					Type:    "work",
					Value:   "user1@test.com",
					Primary: true,
				},
			},
		},
		{
			DisplayName: "user2",
			Id:          "idUser2",
			UserName:    "username2",
			Emails: []iam.ComplexValue{
				{
					Type:    "work",
					Value:   "user2@test.com",
					Primary: true,
				},
			},
		},
	})).Once()

	mockRepo.EXPECT().ListServicePrincipals(mock.Anything).Return(repo.ArrayToChannel([]iam.ServicePrincipal{
		{
			Active:        true,
			ApplicationId: "someApplicationId1",
			DisplayName:   "Service Principal 1",
			Id:            "ServicePrincipalId1",
		},
		{
			Active:        true,
			ApplicationId: "someApplicationId2",
			DisplayName:   "Service Principal 2",
			Id:            "ServicePrincipalId2",
		},
	}))

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			constants.DatabricksAccountId: "AccountId",
			constants.DatabricksUser:      "User",
			constants.DatabricksPassword:  "Password",
			constants.DatabricksPlatform:  "AWS",
		},
	}

	// When
	err := service.SyncIdentityStore(context.Background(), identityHandlerMock, configMap)

	// Then
	require.NoError(t, err)

	assert.ElementsMatch(t, identityHandlerMock.Users, []identity_store.User{
		{
			Name:             "user1",
			Email:            "user1@test.com",
			ExternalId:       "idUser1",
			UserName:         "username1",
			GroupExternalIds: []string{"gid1"},
		},
		{
			Name:             "user2",
			Email:            "user2@test.com",
			ExternalId:       "idUser2",
			UserName:         "username2",
			GroupExternalIds: []string{"gid2"},
		},
		{
			Name:       "Service Principal 1",
			Email:      "someApplicationId1",
			ExternalId: "ServicePrincipalId1",
			UserName:   "someApplicationId1",
			GroupExternalIds: []string{
				"gid2",
			},
		},
		{
			Name:       "Service Principal 2",
			Email:      "someApplicationId2",
			ExternalId: "ServicePrincipalId2",
			UserName:   "someApplicationId2",
		},
	})

	assert.Equal(t, []identity_store.Group{
		{
			Name:                   "group-1",
			DisplayName:            "group-1",
			ExternalId:             "gid1",
			ParentGroupExternalIds: []string{"gid2"},
		},
		{
			Name:                   "group-2",
			DisplayName:            "group-2",
			ExternalId:             "gid2",
			ParentGroupExternalIds: []string(nil),
		},
	}, identityHandlerMock.Groups)
}

func createIdentityStoreSyncer(t *testing.T) (*IdentityStoreSyncer, *mockIdentityStoreAccountRepository) {
	t.Helper()

	repo := newMockIdentityStoreAccountRepository(t)

	return &IdentityStoreSyncer{accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *repo2.RepositoryCredentials) (identityStoreAccountRepository, error) {
		return repo, nil
	}}, repo
}
