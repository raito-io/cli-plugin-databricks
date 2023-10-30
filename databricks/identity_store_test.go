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

	"cli-plugin-databricks/utils/array"
)

func TestIdentityStoreSyncer_SyncIdentityStore(t *testing.T) {
	// Given
	service, mockRepo := createIdentityStoreSyncer(t)
	identityHandlerMock := mocks.NewSimpleIdentityStoreIdentityHandler(t, 1)

	mockRepo.EXPECT().ListGroups(mock.Anything).Return(array.ArrayToChannel([]interface{}{
		iam.Group{
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
			},
		},
		iam.Group{
			Id:          "gid1",
			DisplayName: "group-1",
			Members: []iam.ComplexValue{{
				Value:   "idUser1",
				Display: "user1",
				Ref:     "Users/idUser1",
			}},
		},
	})).Once()

	mockRepo.EXPECT().ListUsers(mock.Anything).Return(array.ArrayToChannel([]interface{}{
		iam.User{
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
		iam.User{
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

	configMap := &config.ConfigMap{
		Parameters: map[string]string{
			DatabricksAccountId: "AccountId",
			DatabricksUser:      "User",
			DatabricksPassword:  "Password",
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

	return &IdentityStoreSyncer{accountRepoFactory: func(accountId string, repoCredentials RepositoryCredentials) identityStoreAccountRepository {
		return repo
	}}, repo
}
