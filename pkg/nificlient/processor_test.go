package nificlient

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"github.com/stretchr/testify/assert"
)

func TestUpdateProcessor(t *testing.T) {
	assert := assert.New(t)

	id := "16cfd2ec-0174-1000-0000-00004b9b35cc"

	mockEntity := MockProcessor(id)

	entity, err := testUpdateProcessor(t, mockEntity, 200)
	assert.Nil(err)
	assert.NotNil(entity)

	entity, err = testUpdateProcessor(t, mockEntity, 404)
	assert.IsType(ErrNifiClusterReturned404, err)
	assert.Nil(entity)

	entity, err = testUpdateProcessor(t, mockEntity, 500)
	assert.IsType(ErrNifiClusterNotReturned200, err)
	assert.Nil(entity)
}

func TestUpdateProcessorRunStatus(t *testing.T) {
	assert := assert.New(t)

	id := "16cfd2ec-0174-1000-0000-00004b9b35cc"

	mockEntity := MockProcessorRunStatus("Stopped")

	entity, err := testUpdateProcessorRunStatus(t, mockEntity, id, 200)
	assert.Nil(err)
	assert.NotNil(entity)

	entity, err = testUpdateProcessorRunStatus(t, mockEntity, id, 404)
	assert.IsType(ErrNifiClusterReturned404, err)
	assert.Nil(entity)

	entity, err = testUpdateProcessorRunStatus(t, mockEntity, id, 500)
	assert.IsType(ErrNifiClusterNotReturned200, err)
	assert.Nil(entity)
}

func testUpdateProcessorRunStatus(t *testing.T, entity nigoapi.ProcessorRunStatusEntity, id string, status int) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error) {

	cluster := testClusterMock(t)

	client, err := testClientFromCluster(cluster, false)
	if err != nil {
		return nil, err
	}

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	url := nifiAddress(cluster, fmt.Sprintf("/processors/%s/run-status", id))
	httpmock.RegisterResponder(http.MethodPut, url,
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(
				status,
				entity)
		})

	return client.UpdateProcessorRunStatus(id, entity)
}

func testUpdateProcessor(t *testing.T, entity nigoapi.ProcessorEntity, status int) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error) {

	cluster := testClusterMock(t)

	client, err := testClientFromCluster(cluster, false)
	if err != nil {
		return nil, err
	}

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	url := nifiAddress(cluster, fmt.Sprintf("/processors/%s", entity.Id))
	httpmock.RegisterResponder(http.MethodPut, url,
		func(req *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(
				status,
				entity)
		})

	return client.UpdateProcessor(entity)
}

func MockProcessorRunStatus(state string) nigoapi.ProcessorRunStatusEntity {
	var version int64 = 10
	return nigoapi.ProcessorRunStatusEntity{
		Revision: &nigoapi.RevisionDto{Version: &version},
		State:    state,
	}
}

func MockProcessor(id string) nigoapi.ProcessorEntity {
	var version int64 = 10
	return nigoapi.ProcessorEntity{
		Id: id,
		Position: &nigoapi.PositionDto{
			X: 44,
			Y: 55,
		},
		Revision: &nigoapi.RevisionDto{Version: &version},
	}
}
