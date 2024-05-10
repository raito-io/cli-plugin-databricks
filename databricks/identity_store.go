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
	"cli-plugin-databricks/databricks/repo/types"
	utils2 "cli-plugin-databricks/databricks/utils"
	"cli-plugin-databricks/utils"
)

var _ wrappers.IdentityStoreSyncer = (*IdentityStoreSyncer)(nil)

//go:generate go run github.com/vektra/mockery/v2 --name=identityStoreAccountRepository
type identityStoreAccountRepository interface {
	ListUsers(ctx context.Context, optFn ...func(options *types.DatabricksUsersFilter)) <-chan repo.ChannelItem[iam.User]
	ListGroups(ctx context.Context, optFn ...func(options *types.DatabricksGroupsFilter)) <-chan repo.ChannelItem[iam.Group]
	ListServicePrincipals(ctx context.Context, optFn ...func(options *types.DatabricksServicePrincipalFilter)) <-chan repo.ChannelItem[iam.ServicePrincipal]
}

type IdentityStoreSyncer struct {
	accountRepoFactory func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *types.RepositoryCredentials) (identityStoreAccountRepository, error)
}

func NewIdentityStoreSyncer() *IdentityStoreSyncer {
	return &IdentityStoreSyncer{
		accountRepoFactory: func(pltfrm platform.DatabricksPlatform, accountId string, repoCredentials *types.RepositoryCredentials) (identityStoreAccountRepository, error) {
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
	pltfrm, accountId, repoCredentials, err := utils2.GetAndValidateParameters(configMap)
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
		if groupItem.HasError() {
			return nil, fmt.Errorf("list group item: %w", groupItem.Error())
		}

		group := groupItem.Item()
		membergroups := make([]string, 0, len(group.Members))

		for _, member := range group.Members {
			if strings.HasPrefix(member.Ref, "Groups/") {
				membergroups = append(membergroups, member.Value)
				groupParents[member.Value] = append(groupParents[member.Value], group.Id)
			} else {
				userParents[member.Value] = append(userParents[member.Value], group.Id)
			}
		}

		err := dependencyTree.AddDependency(group.Id, membergroups...)
		if err != nil {
			return nil, fmt.Errorf("add member groups to dependency tree: %w", err)
		}

		groupMap[group.Id] = group
	}

	err := dependencyTree.DependencyCleanup()
	if err != nil {
		return nil, fmt.Errorf("dependency cleanup: %w", err)
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
		return nil, fmt.Errorf("breadth first traversal: %w", err)
	}

	return userParents, nil
}

func (i *IdentityStoreSyncer) getUsers(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, userParentMap map[string][]string, repo identityStoreAccountRepository) error {
	channelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	usersChannel := repo.ListUsers(channelCtx)

	for userItem := range usersChannel {
		if userItem.HasError() {
			return fmt.Errorf("list user item: %w", userItem.Error())
		}

		user := userItem.Item()

		var primaryEmail string

		for _, email := range user.Emails {
			if email.Primary {
				primaryEmail = email.Value
				break
			}
		}

		if primaryEmail == "" {
			return fmt.Errorf("user %s has no primary email", user.Name)
		}

		name := user.DisplayName
		if name == "" {
			if user.UserName != "" {
				logger.Warn(fmt.Sprintf("user %s has no display name. Will use username instead.", user.Id))
				name = user.UserName
			} else {
				logger.Warn(fmt.Sprintf("user %s has no display name. Will use id instead.", user.Id))
				name = user.Id
			}
		}

		err := identityHandler.AddUsers(&is.User{
			Name:             name,
			Email:            primaryEmail,
			ExternalId:       user.Id,
			UserName:         user.UserName,
			GroupExternalIds: userParentMap[user.Id],
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (i *IdentityStoreSyncer) getServicePrincipals(ctx context.Context, identityHandler wrappers.IdentityStoreIdentityHandler, userParentMap map[string][]string, repo identityStoreAccountRepository) error {
	channelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	servicePrincipalsChannel := repo.ListServicePrincipals(channelCtx)

	for servicePrincipalItem := range servicePrincipalsChannel {
		if servicePrincipalItem.HasError() {
			return fmt.Errorf("list service principal item: %w", servicePrincipalItem.Error())
		}

		sp := servicePrincipalItem.Item()
		name := sp.DisplayName

		if name == "" {
			logger.Warn(fmt.Sprintf("Service Principal %s has no display name. Will use id instead.", sp.Id))
			name = sp.Id
		}

		err := identityHandler.AddUsers(&is.User{
			Name:             name,
			Email:            sp.ApplicationId,
			ExternalId:       sp.Id,
			UserName:         sp.ApplicationId,
			GroupExternalIds: userParentMap[sp.Id],
		})

		if err != nil {
			return err
		}
	}

	return nil
}
