package databricks

import (
	"context"
	"fmt"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/iam"
	is "github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"

	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/utils"
)

var _ wrappers.IdentityStoreSyncer = (*IdentityStoreSyncer)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=identityStoreAccountRepository
type identityStoreAccountRepository interface {
	ListUsers(ctx context.Context, optFn ...func(options *repo.DatabricksUsersFilter)) <-chan interface{}
	ListGroups(ctx context.Context, optFn ...func(options *repo.DatabricksGroupsFilter)) <-chan interface{}
}

type IdentityStoreSyncer struct {
	accountRepoFactory func(accountId string, repoCredentials *repo.RepositoryCredentials) identityStoreAccountRepository
}

func NewIdentityStoreSyncer() *IdentityStoreSyncer {
	return &IdentityStoreSyncer{
		accountRepoFactory: func(accountId string, repoCredentials *repo.RepositoryCredentials) identityStoreAccountRepository {
			return repo.NewAccountRepository(repoCredentials, accountId)
		},
	}
}

func (i *IdentityStoreSyncer) GetIdentityStoreMetaData(_ context.Context, _ *config.ConfigMap) (*is.MetaData, error) {
	return &is.MetaData{
		Type:        "databricks",
		CanBeMaster: false,
		CanBeLinked: false,
	}, nil
}

func (i *IdentityStoreSyncer) SyncIdentityStore(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, configMap *config.ConfigMap) error {
	accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	accountRepo := i.accountRepoFactory(accountId, &repoCredentials)

	userMemberMap, err := i.getGroups(ctx, identityHandler, accountRepo)
	if err != nil {
		return err
	}

	err = i.getUsers(ctx, identityHandler, userMemberMap, accountRepo)

	return err
}

func (i *IdentityStoreSyncer) getGroups(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, repo identityStoreAccountRepository) (map[string][]string, error) {
	channelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	groupsChannel := repo.ListGroups(channelCtx)

	dependencyTree := utils.NewDependencyTree[string]()
	groupMap := make(map[string]iam.Group)
	groupParents := make(map[string][]string)
	userParents := make(map[string][]string)

	for groupItem := range groupsChannel {
		switch item := groupItem.(type) {
		case error:
			return nil, item
		case iam.Group:
			membergroups := make([]string, 0, len(item.Members))

			for _, member := range item.Members {
				if strings.HasPrefix(member.Ref, "Groups/") {
					membergroups = append(membergroups, member.Value)
					groupParents[member.Value] = append(groupParents[member.Value], item.Id)
				} else if strings.HasPrefix(member.Ref, "Users/") {
					userParents[member.Value] = append(userParents[member.Value], item.Id)
				}
			}

			err := dependencyTree.AddDependency(item.Id, membergroups...)
			if err != nil {
				return nil, err
			}

			groupMap[item.Id] = item
		}
	}

	err := dependencyTree.DependencyCleanup()
	if err != nil {
		return nil, err
	}

	err = dependencyTree.BreadthFirstTraversal(func(groupId string) error {
		group := groupMap[groupId]

		return identityHandler.AddGroups(&is.Group{
			Name:                   group.DisplayName,
			DisplayName:            group.DisplayName,
			ExternalId:             group.Id,
			ParentGroupExternalIds: groupParents[groupId],
		})
	})

	if err != nil {
		return nil, err
	}

	return userParents, nil
}

func (i *IdentityStoreSyncer) getUsers(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, userParentMap map[string][]string, repo identityStoreAccountRepository) error {
	channelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	usersChannel := repo.ListUsers(channelCtx)

	for userItem := range usersChannel {
		switch item := userItem.(type) {
		case error:
			return item
		case iam.User:
			var primaryEmail string

			for _, email := range item.Emails {
				if email.Primary {
					primaryEmail = email.Value
					break
				}
			}

			if primaryEmail == "" {
				return fmt.Errorf("user %s has no primary email", item.Name)
			}

			err := identityHandler.AddUsers(&is.User{
				Name:             item.DisplayName,
				Email:            primaryEmail,
				ExternalId:       item.Id,
				UserName:         item.UserName,
				GroupExternalIds: userParentMap[item.Id],
			})

			if err != nil {
				return err
			}
		}
	}

	return nil
}
