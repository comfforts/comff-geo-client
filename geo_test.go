package geo

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	comffC "github.com/comfforts/comff-constants"
	geo_v1 "github.com/comfforts/comff-geo/api/v1"
	"github.com/comfforts/logger"
)

const TEST_DIR = "data"

func TestGeoClient(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		gc Client,
	){
		"geo location, succeeds": testGeoLocate,
		"geo CRUD, succeeds":     testGeoCRUD,
		"address CRUD, succeeds": testAddressCRUD,
	} {
		t.Run(scenario, func(t *testing.T) {
			gc, teardown := setup(t, logger)
			defer teardown()
			fn(t, gc)
		})
	}

}

func setup(t *testing.T, logger logger.AppLogger) (
	gc Client,
	teardown func(),
) {
	t.Helper()

	clientOpts := NewDefaultClientOption()
	clientOpts.Caller = "geo-client-test"

	gc, err := NewClient(logger, clientOpts)
	require.NoError(t, err)

	return gc, func() {
		t.Logf(" TestGeoClient ended, will close geo client")
		err := gc.Close()
		require.NoError(t, err)
	}
}

func testGeoLocate(t *testing.T, gc Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp, err := gc.GeoLocate(ctx, &geo_v1.GeoRequest{
		PostalCode: comffC.P94952,
		Country:    comffC.US,
	})
	require.NoError(t, err)
	require.Equal(t, 38, int(math.Round(float64(resp.Point.Latitude))))
	require.Equal(t, -123, int(math.Round(float64(resp.Point.Longitude))))
}

func testGeoCRUD(t *testing.T, gc Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	refId, hash := "geo-crud-test@gmail.com", "cR34t3G30"
	addResp, err := gc.AddGeo(ctx, &geo_v1.AddGeoLocationRequest{
		Hash: hash,
		Id:   refId,
	})
	require.NoError(t, err)
	require.Equal(t, hash, addResp.Location.Hash)
	require.Equal(t, refId, addResp.Location.Id)

	getResp, err := gc.GetGeo(ctx, &geo_v1.GetGeoLocationRequest{
		Id: refId,
	})
	require.NoError(t, err)
	require.Equal(t, hash, getResp.Location.Hash)
	require.Equal(t, refId, getResp.Location.Id)

	delResp, err := gc.DeleteGeo(ctx, &geo_v1.DeleteGeoLocationRequest{
		Id:   refId,
		Hash: hash,
	})
	require.NoError(t, err)
	require.Equal(t, true, delResp.Ok)
}

func testAddressCRUD(t *testing.T, gc Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rqstr, refId, st := "address-crud-test@gmail.com", "cR34t3a44r", "212 2nd St."
	addResp, err := gc.AddAddress(ctx, &geo_v1.AddressRequest{
		RequestedBy: rqstr,
		RefId:       refId,
		Type:        geo_v1.AddressType_SHOP,
		Street:      st,
		City:        comffC.PETALUMA,
		PostalCode:  comffC.P94952,
		State:       comffC.CA,
		Country:     comffC.US,
	})
	require.NoError(t, err)
	require.Equal(t, refId, addResp.Address.RefId)
	require.Equal(t, geo_v1.AddressType_SHOP, addResp.Address.Type)

	_, err = gc.GetAddress(ctx, &geo_v1.GetAddressRequest{
		Id: addResp.Address.Id,
	})
	require.NoError(t, err)

	delResp, err := gc.DeleteAddress(ctx, &geo_v1.DeleteAddressRequest{
		Id:    addResp.Address.Id,
		RefId: refId,
	})
	require.NoError(t, err)
	require.Equal(t, true, delResp.Ok)
}
