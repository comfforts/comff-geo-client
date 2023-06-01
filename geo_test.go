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
		"geo location, succeeds":                     testGeoLocate,
		"geo CRUD, succeeds":                         testGeoCRUD,
		"address CRUD, succeeds":                     testAddressCRUD,
		"get routes by addrStr/latLongStr, succeeds": testGetRouteAddrStr,
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

func testGetRouteAddrStr(t *testing.T, gc Client) {
	t.Helper()

	start := &geo_v1.GeoRequest{
		PostalCode: "95476",
		Country:    comffC.US,
		Street:     "641 Ave Del Oro",
		City:       "Sonoma",
		State:      comffC.CA,
	}

	addr := &geo_v1.GeoRequest{
		PostalCode: "95476",
		Country:    comffC.US,
		Street:     "20511 Broadway",
		City:       "Sonoma",
		State:      comffC.CA,
	}

	end := &geo_v1.GeoRequest{
		PostalCode: "95476",
		Country:    comffC.US,
		Street:     "110 W Spain St",
		City:       "Sonoma",
		State:      comffC.CA,
	}

	origins := []*geo_v1.GeoRequest{}
	dests := []*geo_v1.GeoRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sPt, err := gc.GeoLocate(ctx, start)
	require.NoError(t, err)
	origins = append(origins, &geo_v1.GeoRequest{
		Latitude:   sPt.Point.Latitude,
		Longitude:  sPt.Point.Longitude,
		Street:     start.Street,
		City:       start.City,
		State:      start.State,
		PostalCode: start.PostalCode,
		Country:    start.Country,
	})

	aPt, err := gc.GeoLocate(ctx, addr)
	require.NoError(t, err)
	origins = append(origins, &geo_v1.GeoRequest{
		Latitude:   aPt.Point.Latitude,
		Longitude:  aPt.Point.Longitude,
		Street:     addr.Street,
		City:       addr.City,
		State:      addr.State,
		PostalCode: addr.PostalCode,
		Country:    addr.Country,
	})
	dests = append(dests, &geo_v1.GeoRequest{
		Latitude:   aPt.Point.Latitude,
		Longitude:  aPt.Point.Longitude,
		Street:     addr.Street,
		City:       addr.City,
		State:      addr.State,
		PostalCode: addr.PostalCode,
		Country:    addr.Country,
	})

	ePt, err := gc.GeoLocate(ctx, end)
	require.NoError(t, err)
	dests = append(dests, &geo_v1.GeoRequest{
		Latitude:   ePt.Point.Latitude,
		Longitude:  ePt.Point.Longitude,
		Street:     end.Street,
		City:       end.City,
		State:      end.State,
		PostalCode: end.PostalCode,
		Country:    end.Country,
	})

	resp, err := gc.GetGeoRoute(ctx, &geo_v1.GeoRouteRequest{
		Origins:      origins,
		Destinations: dests,
		IsLatLng:     true,
	})
	require.NoError(t, err)
	require.Equal(t, true, len(resp.RouteLegs) == 4)

	resp, err = gc.GetGeoRoute(ctx, &geo_v1.GeoRouteRequest{
		Origins:      origins,
		Destinations: dests,
		IsLatLng:     false,
	})
	require.NoError(t, err)
	require.Equal(t, true, len(resp.RouteLegs) == 4)
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
