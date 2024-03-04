package databricks

import (
	"context"
	"fmt"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/iam"
	is "github.com/raito-io/cli/base/identity_store"
	"github.com/raito-io/cli/base/util/config"
	"github.com/raito-io/cli/base/wrappers"

	"cli-plugin-databricks/databricks/platform"
	"cli-plugin-databricks/databricks/repo"
	"cli-plugin-databricks/utils"
)

var _ wrappers.IdentityStoreSyncer = (*IdentityStoreSyncer)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=identityStoreAccountRepository
type identityStoreAccountRepository interface {
	ListUsers(ctx context.Context, optFn ...func(options *repo.DatabricksUsersFilter)) <-chan interface{}
	ListGroups(ctx context.Context, optFn ...func(options *repo.DatabricksGroupsFilter)) <-chan interface{}
	ListServicePrincipals(ctx context.Context, optFn ...func(options *repo.DatabricksServicePrincipalFilter)) <-chan interface{}
}

type IdentityStoreSyncer struct {
	accountRepoFactory func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *repo.RepositoryCredentials) (identityStoreAccountRepository, error)
}

func NewIdentityStoreSyncer() *IdentityStoreSyncer {
	return &IdentityStoreSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *repo.RepositoryCredentials) (identityStoreAccountRepository, error) {
			return repo.NewAccountRepository(pltfrm, repoCredentials, accountId)
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
	pltfrm, accountId, repoCredentials, err := getAndValidateParameters(configMap)
	if err != nil {
		return err
	}

	accountRepo, err := i.accountRepoFactory(pltfrm, accountId, &repoCredentials)
	if err != nil {
		return fmt.Errorf("account repository factory: %w", err)
	}

	userMemberMap, err := i.getGroups(ctx, identityHandler, accountRepo)
	if err != nil {
		return fmt.Errorf("load groups: %w", err)
	}

	err = i.getUsers(ctx, identityHandler, userMemberMap, accountRepo)
	if err != nil {
		return fmt.Errorf("load users: %w", err)
	}

	err = i.getServicePrincipals(ctx, identityHandler, userMemberMap, accountRepo)
	if err != nil {
		return fmt.Errorf("load service principals: %w", err)
	}

	return nil
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
				} else {
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

			name := item.DisplayName
			if name == "" {
				if item.UserName != "" {
					logger.Warn(fmt.Sprintf("user %s has no display name. Will use username instead.", item.Id))
					name = item.UserName
				} else {
					logger.Warn(fmt.Sprintf("user %s has no display name. Will use id instead.", item.Id))
					name = item.Id
				}
			}

			err := identityHandler.AddUsers(&is.User{
				Name:             name,
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

func (i *IdentityStoreSyncer) getServicePrincipals(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, userParentMap map[string][]string, repo identityStoreAccountRepository) error {
	channelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	servicePrincipalsChannel := repo.ListServicePrincipals(channelCtx)

	for servicePrincipalItem := range servicePrincipalsChannel {
		switch item := servicePrincipalItem.(type) {
		case error:
			return item
		case iam.ServicePrincipal:
			name := item.DisplayName

			if name == "" {
				logger.Warn(fmt.Sprintf("Service Principal %s has no display name. Will use id instead.", item.Id))
				name = item.Id
			}

			err := identityHandler.AddUsers(&is.User{
				Name:             name,
				Email:            item.ApplicationId,
				ExternalId:       item.Id,
				UserName:         item.ApplicationId,
				GroupExternalIds: userParentMap[item.Id],
			})

			if err != nil {
				return err
			}
		}
	}

	return nil
}
