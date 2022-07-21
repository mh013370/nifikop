package accesspolicies

import (
	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

var log = common.CustomLogger().Named("accesspolicies-method")

func ExistAccessPoliciesWithClient(accessPolicy *v1alpha1.AccessPolicy, config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (bool, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return false, err
	}

	entity, err := nClient.GetAccessPolicyWithClient(string(accessPolicy.Action), accessPolicy.GetResource(config.RootProcessGroupId), client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get access policy"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return false, nil
		}
		return false, err
	}

	return entity != nil && entity.Entity != nil && entity.Entity.Id != "", nil
}

func CreateAccessPolicyWithClient(accessPolicy *v1alpha1.AccessPolicy, config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (string, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return "", err
	}

	scratchEntity := nigoapi.AccessPolicyEntity{}
	updateAccessPolicyEntity(
		accessPolicy,
		[]*v1alpha1.NifiUser{}, []*v1alpha1.NifiUser{},
		[]*v1alpha1.NifiUserGroup{}, []*v1alpha1.NifiUserGroup{},
		config,
		&scratchEntity)

	entity, err := nClient.CreateAccessPolicyWithClient(scratchEntity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Access policy user"); err != nil {
		return "", err
	}

	return entity.Entity.Id, nil
}

func UpdateAccessPolicyWithClient(
	accessPolicy *v1alpha1.AccessPolicy,
	addUsers []*v1alpha1.NifiUser,
	removeUsers []*v1alpha1.NifiUser,
	addUserGroups []*v1alpha1.NifiUserGroup,
	removeUserGroups []*v1alpha1.NifiUserGroup,
	config *clientconfig.NifiConfig,
	client nificlient.ClientContextPair) error {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	log.Info("Updating access policy",
		zap.Int32("nodeId", client.NodeId),
		zap.String("accessResource", string(accessPolicy.Resource)),
		zap.String("accessAction", string(accessPolicy.Action)),
		zap.String("rootPgId", config.RootProcessGroupId),
		zap.String("accessType", string(accessPolicy.Type)))

	// Check if the access policy  exist
	exist, err := ExistAccessPoliciesWithClient(accessPolicy, config, client)
	if err != nil {
		return err
	}

	if !exist {
		log.Info("Creating access policy, as it does not exist",
			zap.Int32("nodeId", client.NodeId),
			zap.String("accessResource", string(accessPolicy.Resource)),
			zap.String("accessAction", string(accessPolicy.Action)),
			zap.String("rootPgId", config.RootProcessGroupId),
			zap.String("accessType", string(accessPolicy.Type)))
		_, err = CreateAccessPolicyWithClient(accessPolicy, config, client)
		if err != nil {
			return err
		}
	}

	entity, err := nClient.GetAccessPolicyWithClient(string(accessPolicy.Action), accessPolicy.GetResource(config.RootProcessGroupId), client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get access policy"); err != nil {
		return err
	}

	updateAccessPolicyEntity(accessPolicy, addUsers, removeUsers, addUserGroups, removeUserGroups, config, entity.Entity)
	_, err = nClient.UpdateAccessPolicyWithClient(*entity.Entity, client)
	return clientwrappers.ErrorUpdateOperation(log, err, "Update access policy")
}

func UpdateAccessPolicyEntityWithClient(
	entity *nigoapi.AccessPolicyEntity,
	addUsers []*v1alpha1.NifiUser,
	removeUsers []*v1alpha1.NifiUser,
	addUserGroups []*v1alpha1.NifiUserGroup,
	removeUserGroups []*v1alpha1.NifiUserGroup,
	config *clientconfig.NifiConfig,
	client nificlient.ClientContextPair) error {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	updatedEntity, err := nClient.GetAccessPolicyWithClient(entity.Component.Action, entity.Component.Resource, client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get access policy"); err != nil {
		return err
	}
	entity = updatedEntity.Entity

	addRemoveUsersFromAccessPolicyEntity(addUsers, removeUsers, entity)
	addRemoveUserGroupsFromAccessPolicyEntity(addUserGroups, removeUserGroups, entity)

	_, err = nClient.UpdateAccessPolicyWithClient(*entity, client)
	return clientwrappers.ErrorUpdateOperation(log, err, "Update user")
}

func updateAccessPolicyEntity(
	accessPolicy *v1alpha1.AccessPolicy,
	addUsers []*v1alpha1.NifiUser,
	removeUsers []*v1alpha1.NifiUser,
	addUserGroups []*v1alpha1.NifiUserGroup,
	removeUserGroups []*v1alpha1.NifiUserGroup,
	config *clientconfig.NifiConfig,
	entity *nigoapi.AccessPolicyEntity) {

	var defaultVersion int64 = 0

	if entity == nil {
		entity = &nigoapi.AccessPolicyEntity{}
	}

	if entity.Component == nil {
		entity.Revision = &nigoapi.RevisionDto{
			Version: &defaultVersion,
		}
	}

	if entity.Component == nil {
		entity.Component = &nigoapi.AccessPolicyDto{}
	}

	entity.Component.Action = string(accessPolicy.Action)
	entity.Component.Resource = accessPolicy.GetResource(config.RootProcessGroupId)

	addRemoveUsersFromAccessPolicyEntity(addUsers, removeUsers, entity)
	addRemoveUserGroupsFromAccessPolicyEntity(addUserGroups, removeUserGroups, entity)
}

func addRemoveUserGroupsFromAccessPolicyEntity(
	addUserGroups []*v1alpha1.NifiUserGroup,
	removeUserGroups []*v1alpha1.NifiUserGroup,
	entity *nigoapi.AccessPolicyEntity) {

	// Add new userGroup from the access policy
	for _, userGroup := range addUserGroups {
		log.Info("Adding user group to access policy",
			zap.String("userGroup", userGroup.Name),
			zap.String("policy", entity.Component.Resource),
		)
		entity.Component.UserGroups = append(entity.Component.UserGroups, nigoapi.TenantEntity{Id: userGroup.Status.Id})
	}

	// Remove user from the access policy
	var userGroupsAccessPolicy []nigoapi.TenantEntity
	for _, userGroup := range entity.Component.UserGroups {
		contains := false

		for _, toRemove := range removeUserGroups {
			if userGroup.Id == toRemove.Status.Id {
				contains = true
				break
			}
		}

		if !contains {
			userGroupsAccessPolicy = append(userGroupsAccessPolicy, userGroup)
		}
	}
	entity.Component.UserGroups = userGroupsAccessPolicy
}

func addRemoveUsersFromAccessPolicyEntity(
	addUsers []*v1alpha1.NifiUser,
	removeUsers []*v1alpha1.NifiUser,
	entity *nigoapi.AccessPolicyEntity) {

	// Add new user from the access policy
	for _, user := range addUsers {
		log.Info("Adding user to access policy",
			zap.String("user", user.Name),
			zap.String("policy", entity.Component.Resource),
		)
		entity.Component.Users = append(entity.Component.Users, nigoapi.TenantEntity{Id: user.Status.Id})
	}

	// Remove user from the access policy
	var usersAccessPolicy []nigoapi.TenantEntity
	for _, user := range entity.Component.Users {
		contains := false

		for _, toRemove := range removeUsers {
			if user.Id == toRemove.Status.Id {
				contains = true
				break
			}
		}

		if !contains {
			usersAccessPolicy = append(usersAccessPolicy, user)
		}
	}
	entity.Component.Users = usersAccessPolicy
}
