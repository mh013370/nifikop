package usergroup

import (
	"errors"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers/accesspolicies"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers/dataflow"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

var log = common.CustomLogger().Named("usergroup-method")

func CreateIfNotExists(userGroup *v1alpha1.NifiUserGroup,
	users []*v1alpha1.NifiUser, config *clientconfig.NifiConfig) (*v1alpha1.NifiUserGroupStatus, error) {
	// set the seed header if it exists in the parameter context status
	if userGroup.Status.IdSeed != "" {
		config.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER] = userGroup.Status.IdSeed
	}

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	if userGroup.Status.Id == "" {
		// find any user groups which may exist but weren't created by the operator
		existingUserGroups, err := FindUserGroupByIdentity(userGroup, config)
		if err != nil {
			return nil, err
		}
		var status *v1alpha1.NifiUserGroupStatus
		for _, entityPair := range existingUserGroups {
			if entityPair.Entity == nil {
				status = &v1alpha1.NifiUserGroupStatus{
					Id:      entityPair.Entity.Id,
					Version: entityPair.Entity.Version,
				}
			}
		}
		if status == nil {
			status, err = CreateUserGroup(userGroup, users, config)
			if err != nil {
				return nil, err
			}
		}
		return status, nil
	}

	// double check that the user group exists at each configured client
	entities, err := nClient.GetUserGroups()
	if err := clientwrappers.ErrorGetOperation(log, err, "get user groups"); err != nil {
		return nil, err
	}
	var retStatus *v1alpha1.NifiUserGroupStatus
	for _, entityPair := range entities {
		entities := entityPair.Entity
		if !groupExists(userGroup, *entities) {
			status, err := CreateUserGroupWithClient(userGroup, users, config, entityPair.Client)
			if err != nil {
				return nil, err
			}
			retStatus = status
		}
	}

	return retStatus, nil
}

func groupExists(userGroup *v1alpha1.NifiUserGroup, groups []nigoapi.UserGroupEntity) bool {
	for _, entity := range groups {
		if entity.Component.Identity == userGroup.GetIdentity() {
			return true
		}
	}
	return false
}

func FindUserGroupByIdentity(userGroup *v1alpha1.NifiUserGroup, config *clientconfig.NifiConfig) ([]nificlient.ClientEntityPair[v1alpha1.NifiUserGroupStatus], error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	entities, err := nClient.GetUserGroups()
	if err := clientwrappers.ErrorGetOperation(log, err, "Get users"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil, nil
		}
		return nil, err
	}

	retEntities := []nificlient.ClientEntityPair[v1alpha1.NifiUserGroupStatus]{}
	for _, entityPair := range entities {
		for _, entity := range *entityPair.Entity {
			if userGroup.GetIdentity() == entity.Component.Identity {
				retEntities = append(retEntities, nificlient.ClientEntityPair[v1alpha1.NifiUserGroupStatus]{
					Client: entityPair.Client,
					Entity: &v1alpha1.NifiUserGroupStatus{
						Id:      entity.Id,
						Version: *entity.Revision.Version,
					},
				})
			}
		}
	}

	return retEntities, nil
}

func CreateUserGroup(userGroup *v1alpha1.NifiUserGroup,
	users []*v1alpha1.NifiUser, config *clientconfig.NifiConfig) (*v1alpha1.NifiUserGroupStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.UserGroupEntity{}
	updateUserGroupEntity(userGroup, users, &scratchEntity)

	entity, err := nClient.CreateUserGroup(scratchEntity)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create user-group"); err != nil {
		return nil, err
	}

	return &v1alpha1.NifiUserGroupStatus{
		Id:      entity[0].Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entity[0].Entity.Revision.Version,
	}, nil
}

func CreateUserGroupWithClient(userGroup *v1alpha1.NifiUserGroup,
	users []*v1alpha1.NifiUser, config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (*v1alpha1.NifiUserGroupStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.UserGroupEntity{}
	updateUserGroupEntity(userGroup, users, &scratchEntity)

	entity, err := nClient.CreateUserGroupWithClient(scratchEntity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create user-group"); err != nil {
		return nil, err
	}

	return &v1alpha1.NifiUserGroupStatus{
		Id:      entity.Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entity.Entity.Revision.Version,
	}, nil
}

func SyncUserGroup(userGroup *v1alpha1.NifiUserGroup, users []*v1alpha1.NifiUser,
	config *clientconfig.NifiConfig) (*v1alpha1.NifiUserGroupStatus, error) {

	// we set unique id seed header here so each client gets a unique id seed header. This is so that
	// if any access policies get created below, they will each get a distinct ID instead of sharing the same ID.
	config.UniqueIdSeedHeader = true
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}
	// unset the unique id seed header in case the config is used again after this to create clients.
	config.UniqueIdSeedHeader = false

	entities, err := nClient.GetUserGroup(userGroup.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get user-group"); err != nil {
		return nil, err
	}

	var status v1alpha1.NifiUserGroupStatus
	for _, entityPair := range entities {
		if entityPair == nil || entityPair.Entity == nil {
			return nil, errorfactory.NifiUserGroupSyncing{}
		}
		entity := entityPair.Entity

		// if the cluster is standalone, fetch this node's root process group ID since it will be different on each node.
		if config.IsStandalone {
			rootPgId, err := dataflow.RootProcessGroupWithClient(config, entityPair.Client)
			if err != nil {
				return nil, errors.New("failed to fetch root process group id")
			}
			config.RootProcessGroupId = rootPgId
		}

		if !userGroupIsSync(userGroup, users, entity) {
			log.Info("User group is out of sync. Updating.",
				zap.String("clusterName", userGroup.Spec.ClusterRef.Name),
				zap.String("userGroupName", userGroup.Name),
				zap.String("userGroupId", userGroup.Status.Id))
			updateUserGroupEntity(userGroup, users, entity)
			updatedEntity, err := nClient.UpdateUserGroupWithClient(*entity, entityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Update user-group"); err != nil {
				return nil, err
			}
			entity = updatedEntity.Entity
		}

		status = userGroup.Status
		status.Version = *entity.Revision.Version
		status.Id = entity.Id

		// Remove from access policy
		for _, entity := range entity.Component.AccessPolicies {
			contains := userGroupContainsAccessPolicy(userGroup, entity, config.RootProcessGroupId)
			if !contains {
				if err := accesspolicies.UpdateAccessPolicyEntityWithClient(&entity,
					[]*v1alpha1.NifiUser{}, []*v1alpha1.NifiUser{},
					[]*v1alpha1.NifiUserGroup{}, []*v1alpha1.NifiUserGroup{userGroup}, config, entityPair.Client); err != nil {
					return &status, err
				}
			}
		}

		// add
		for _, accessPolicy := range userGroup.Spec.AccessPolicies {
			contains := UserGroupEntityContainsAccessPolicy(entity, accessPolicy, config.RootProcessGroupId)
			if !contains {
				if err := accesspolicies.UpdateAccessPolicyWithClient(&accessPolicy,
					[]*v1alpha1.NifiUser{}, []*v1alpha1.NifiUser{},
					[]*v1alpha1.NifiUserGroup{userGroup}, []*v1alpha1.NifiUserGroup{}, config, entityPair.Client); err != nil {
					return &status, err
				}
			}
		}
	}

	return &status, nil
}

func RemoveUserGroup(userGroup *v1alpha1.NifiUserGroup, users []*v1alpha1.NifiUser, config *clientconfig.NifiConfig) error {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	entities, err := nClient.GetUserGroup(userGroup.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get user-group"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil
		}
		return err
	}

	for _, entityPair := range entities {
		if entityPair != nil && entityPair.Entity != nil {
			entity := entityPair.Entity
			updateUserGroupEntity(userGroup, users, entity)
			err = nClient.RemoveUserGroup(*entity)
			if err != nil {
				return clientwrappers.ErrorRemoveOperation(log, err, "Remove user-group")
			}
		}
	}

	return nil
}

func userGroupIsSync(
	userGroup *v1alpha1.NifiUserGroup,
	users []*v1alpha1.NifiUser,
	entity *nigoapi.UserGroupEntity) bool {

	if userGroup.GetIdentity() != entity.Component.Identity {
		return false
	}

	for _, expected := range users {
		notFound := true
		for _, tenant := range entity.Component.Users {
			if expected.Status.Id == tenant.Id {
				notFound = false
				break
			}
		}
		if notFound {
			return false
		}
	}
	return true
}

func updateUserGroupEntity(userGroup *v1alpha1.NifiUserGroup, users []*v1alpha1.NifiUser, entity *nigoapi.UserGroupEntity) {

	var defaultVersion int64 = 0

	if entity == nil {
		entity = &nigoapi.UserGroupEntity{}
	}

	if entity.Component == nil {
		entity.Revision = &nigoapi.RevisionDto{
			Version: &defaultVersion,
		}
	}

	if entity.Component == nil {
		entity.Component = &nigoapi.UserGroupDto{}
	}

	entity.Component.Identity = userGroup.GetIdentity()

	for _, user := range users {
		entity.Component.Users = append(entity.Component.Users, nigoapi.TenantEntity{Id: user.Status.Id})
	}
}

func userGroupContainsAccessPolicy(userGroup *v1alpha1.NifiUserGroup, entity nigoapi.AccessPolicyEntity, rootPGId string) bool {
	for _, accessPolicy := range userGroup.Spec.AccessPolicies {
		if entity.Component.Action == string(accessPolicy.Action) &&
			entity.Component.Resource == accessPolicy.GetResource(rootPGId) {
			return true
		}
	}
	return false
}

func UserGroupEntityContainsAccessPolicy(entity *nigoapi.UserGroupEntity, accessPolicy v1alpha1.AccessPolicy, rootPGId string) bool {
	for _, entity := range entity.Component.AccessPolicies {
		if entity.Component.Action == string(accessPolicy.Action) &&
			entity.Component.Resource == accessPolicy.GetResource(rootPGId) {
			return true
		}
	}
	return false
}
