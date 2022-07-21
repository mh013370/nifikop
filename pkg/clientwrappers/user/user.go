package user

import (
	"errors"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers/accesspolicies"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers/dataflow"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers/usergroup"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

var log = common.CustomLogger().Named("user-method")

func CreateIfNotExists(user *v1alpha1.NifiUser, config *clientconfig.NifiConfig) (*v1alpha1.NifiUserStatus, error) {
	// set the seed header if it exists in the parameter context status
	if user.Status.IdSeed != "" {
		config.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER] = user.Status.IdSeed
	}

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	if user.Status.Id == "" {
		// find any users which may exist but weren't created by the operator
		existingUsers, err := FindUserByIdentity(user, config)
		if err != nil {
			return nil, err
		}
		var status *v1alpha1.NifiUserStatus
		for _, entityPair := range existingUsers {
			if entityPair.Entity != nil {
				status = &v1alpha1.NifiUserStatus{
					Id:      entityPair.Entity.Id,
					Version: entityPair.Entity.Version,
				}
			}
		}
		if status == nil {
			status, err = CreateUser(user, config)
			if err != nil {
				return nil, err
			}
		}
		return status, nil
	}

	// double check that the user exists at each configured client
	entities, err := nClient.GetUser(user.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "get user"); err != nil {
		return nil, err
	}
	var retStatus *v1alpha1.NifiUserStatus
	for _, entity := range entities {
		if entity == nil || entity.Entity == nil {
			userStatus, _ := FindUserByIdentityWithClient(user, config, entity.Client)
			// the user already exists. So remove it and re-create with the correct seed.
			if userStatus.Entity != nil {
				log.Warn("User already exists. Removing and re-creating with correct seed.",
					zap.Int32("nodeId", entity.Client.NodeId),
					zap.String("userName", user.Name),
					zap.String("userId", user.Status.Id),
					zap.String("oldUserId", userStatus.Entity.Id),
					zap.String("userIdSeed", user.Status.IdSeed),
					zap.String("clusterName", user.Spec.ClusterRef.Name),
				)
				userToRemove := &nigoapi.UserEntity{}
				updateUserEntity(user, userToRemove)
				err = nClient.RemoveUserWithClient(*userToRemove, entity.Client)
				if err := clientwrappers.ErrorGetOperation(log, err, "remove user"); err != nil {
					return nil, err
				}
			}

			status, err := CreateUserWithClient(user, config, entity.Client)
			if err != nil {
				return nil, err
			}
			retStatus = status
		}
	}

	return retStatus, nil
}

func FindUserByIdentityWithClient(user *v1alpha1.NifiUser, config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (nificlient.ClientEntityPair[v1alpha1.NifiUserStatus], error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nificlient.ClientEntityPair[v1alpha1.NifiUserStatus]{}, err
	}

	entityPair, err := nClient.GetUsersWithClient(client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get users"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nificlient.ClientEntityPair[v1alpha1.NifiUserStatus]{}, nil
		}
		return nificlient.ClientEntityPair[v1alpha1.NifiUserStatus]{}, err
	}

	for _, entity := range *entityPair.Entity {
		if user.GetIdentity() == entity.Component.Identity {
			return nificlient.ClientEntityPair[v1alpha1.NifiUserStatus]{
				Client: entityPair.Client,
				Entity: &v1alpha1.NifiUserStatus{
					Id:      entity.Id,
					Version: *entity.Revision.Version,
				},
			}, nil
		}
	}

	return nificlient.ClientEntityPair[v1alpha1.NifiUserStatus]{}, nil
}

func FindUserByIdentity(user *v1alpha1.NifiUser, config *clientconfig.NifiConfig) ([]nificlient.ClientEntityPair[v1alpha1.NifiUserStatus], error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	entities, err := nClient.GetUsers()
	if err := clientwrappers.ErrorGetOperation(log, err, "Get users"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil, nil
		}
		return nil, err
	}

	retEntities := []nificlient.ClientEntityPair[v1alpha1.NifiUserStatus]{}
	for _, entityPair := range entities {
		for _, entity := range *entityPair.Entity {
			if user.GetIdentity() == entity.Component.Identity {
				retEntities = append(retEntities, nificlient.ClientEntityPair[v1alpha1.NifiUserStatus]{
					Client: entityPair.Client,
					Entity: &v1alpha1.NifiUserStatus{
						Id:      entity.Id,
						Version: *entity.Revision.Version,
					},
				})
			}
		}
	}

	return retEntities, nil
}

func CreateUser(user *v1alpha1.NifiUser, config *clientconfig.NifiConfig) (*v1alpha1.NifiUserStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.UserEntity{}
	updateUserEntity(user, &scratchEntity)

	entity, err := nClient.CreateUser(scratchEntity)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create user"); err != nil {
		return nil, err
	}

	return &v1alpha1.NifiUserStatus{
		Id:      entity[0].Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entity[0].Entity.Revision.Version,
	}, nil
}

func CreateUserWithClient(user *v1alpha1.NifiUser, config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (*v1alpha1.NifiUserStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.UserEntity{}
	updateUserEntity(user, &scratchEntity)

	entity, err := nClient.CreateUserWithClient(scratchEntity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create user"); err != nil {
		return nil, err
	}

	return &v1alpha1.NifiUserStatus{
		Id:      entity.Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entity.Entity.Revision.Version,
	}, nil
}

func SyncUser(user *v1alpha1.NifiUser, config *clientconfig.NifiConfig) (*v1alpha1.NifiUserStatus, error) {
	// we set unique id seed header here so each client gets a unique id seed header. This is so that
	// if any access policies get created below, they will each get a distinct ID instead of sharing the same ID.
	// This also means that access policy IDs will not be reliably generated between nodes in standalone clusters, but this should be okay.
	config.UniqueIdSeedHeader = true
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}
	// unset the unique id seed header in case the config is used again after this to create clients.
	config.UniqueIdSeedHeader = false

	entities, err := nClient.GetUser(user.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get user"); err != nil {
		return nil, err
	}

	var status v1alpha1.NifiUserStatus
	for _, entityPair := range entities {
		if entityPair == nil || entityPair.Entity == nil {
			return nil, errorfactory.NifiUserSyncing{}
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

		if !userIsSync(user, entity) {
			updateUserEntity(user, entity)
			updatedEntity, err := nClient.UpdateUserWithClient(*entity, entityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Update user"); err != nil {
				return nil, err
			}
			entity = updatedEntity.Entity
		}

		status = user.Status
		status.Version = *entity.Revision.Version
		status.Id = entity.Id

		// Remove from access policy
		for _, ent := range entity.Component.AccessPolicies {
			contains := false
			for _, group := range entity.Component.UserGroups {
				userGroupEntity, err := nClient.GetUserGroupWithClient(group.Id, entityPair.Client)
				if err := clientwrappers.ErrorGetOperation(log, err, "Get user-group"); err != nil {
					return nil, err
				}

				if userGroupEntityContainsAccessPolicyEntity(userGroupEntity.Entity, ent) {
					contains = true
					break
				}
			}
			if !contains && !userContainsAccessPolicy(user, ent, config.RootProcessGroupId) {
				if err := accesspolicies.UpdateAccessPolicyEntityWithClient(
					&nigoapi.AccessPolicyEntity{
						Component: &nigoapi.AccessPolicyDto{
							Id:       ent.Component.Id,
							Resource: ent.Component.Resource,
							Action:   ent.Component.Action,
						},
					},
					[]*v1alpha1.NifiUser{}, []*v1alpha1.NifiUser{user},
					[]*v1alpha1.NifiUserGroup{}, []*v1alpha1.NifiUserGroup{}, config, entityPair.Client); err != nil {
					return &status, err
				}
			}
		}

		// add
		for _, accessPolicy := range user.Spec.AccessPolicies {
			contains := false
			for _, group := range entity.Component.UserGroups {
				userGroupEntity, err := nClient.GetUserGroupWithClient(group.Id, entityPair.Client)
				if err := clientwrappers.ErrorGetOperation(log, err, "Get user-group"); err != nil {
					return nil, err
				}

				if usergroup.UserGroupEntityContainsAccessPolicy(userGroupEntity.Entity, accessPolicy, config.RootProcessGroupId) {
					contains = true
					break
				}
			}
			if !contains && !userEntityContainsAccessPolicy(entity, accessPolicy, config.RootProcessGroupId) {
				if err := accesspolicies.UpdateAccessPolicyWithClient(&accessPolicy,
					[]*v1alpha1.NifiUser{user}, []*v1alpha1.NifiUser{},
					[]*v1alpha1.NifiUserGroup{}, []*v1alpha1.NifiUserGroup{}, config, entityPair.Client); err != nil {
					return &status, err
				}
			}
		}
	}

	return &status, nil
}

func RemoveUser(user *v1alpha1.NifiUser, config *clientconfig.NifiConfig) error {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	entities, err := nClient.GetUser(user.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get user"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil
		}
		return err
	}

	for _, entityPair := range entities {
		if entityPair != nil && entityPair.Entity != nil {
			entity := entityPair.Entity
			updateUserEntity(user, entity)
			err = nClient.RemoveUserWithClient(*entity, entityPair.Client)
			if err != nil {
				return clientwrappers.ErrorRemoveOperation(log, err, "Remove user")
			}
		}
	}
	return nil
}

func userIsSync(user *v1alpha1.NifiUser, entity *nigoapi.UserEntity) bool {
	return user.GetIdentity() == entity.Component.Identity
}

func updateUserEntity(user *v1alpha1.NifiUser, entity *nigoapi.UserEntity) {

	var defaultVersion int64 = 0

	if entity == nil {
		entity = &nigoapi.UserEntity{}
	}

	if entity.Component == nil {
		entity.Revision = &nigoapi.RevisionDto{
			Version: &defaultVersion,
		}
	}

	if entity.Component == nil {
		entity.Component = &nigoapi.UserDto{}
	}

	entity.Component.Identity = user.GetIdentity()
}

func userContainsAccessPolicy(user *v1alpha1.NifiUser, entity nigoapi.AccessPolicySummaryEntity, rootPGId string) bool {
	for _, accessPolicy := range user.Spec.AccessPolicies {
		if entity.Component.Action == string(accessPolicy.Action) &&
			entity.Component.Resource == accessPolicy.GetResource(rootPGId) {
			return true
		}
	}
	return false
}

func userEntityContainsAccessPolicy(entity *nigoapi.UserEntity, accessPolicy v1alpha1.AccessPolicy, rootPGId string) bool {
	for _, entity := range entity.Component.AccessPolicies {
		if entity.Component.Action == string(accessPolicy.Action) &&
			entity.Component.Resource == accessPolicy.GetResource(rootPGId) {
			return true
		}
	}
	return false
}

func userGroupEntityContainsAccessPolicyEntity(entity *nigoapi.UserGroupEntity, accessPolicy nigoapi.AccessPolicySummaryEntity) bool {
	for _, entity := range entity.Component.AccessPolicies {
		if entity.Component.Action == accessPolicy.Component.Action &&
			entity.Component.Resource == accessPolicy.Component.Resource {
			return true
		}
	}
	return false
}
