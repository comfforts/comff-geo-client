package loadbalance_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"

	"github.com/comfforts/comff-geo-client/internal/loadbalance"
)

const SERVICE_HOST = "127.0.0.1"
const TEST_DIR = "data"

func TestPickerNoSubConnectionsErr(t *testing.T) {
	picker := &loadbalance.Picker{}
	for _, method := range []string{
		"/geo.vX.Geo/AddGeoLocation",
		"/geo.vX.Geo/AddGeoLocationLatLong",
		"/geo.vX.Geo/AddGeoLocation",
		"/geo.vX.Geo/AddGeoLocationLatLong",
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}
		result, err := picker.Pick(info)
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

func TestPickLeaderForAddAddress(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "/geo.vX.Geo/AddAddress",
	}
	for i := 0; i < 5; i++ {
		gotPick, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[0], gotPick.SubConn)
	}
}

func TestPickFollowerForGetAddress(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: "/geo.vX.Geo/GetAddress",
	}
	for i := 0; i < 5; i++ {
		pick, err := picker.Pick(info)
		require.NoError(t, err)
		require.NotEqual(t, subConns[0], pick.SubConn)
	}
}

// subConn implements balancer.SubConn.
type subConn struct {
	balancer.SubConn
	addrs []resolver.Address
}

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}

func (s *subConn) Connect() {}

func setupTest() (*loadbalance.Picker, []*subConn) {
	var subConns []*subConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	for i := 0; i < 3; i++ {
		port := getPort(i)

		sc := &subConn{}

		// first sub conn is the leader
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
			Addr:       getAddr(port),
		}
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}
	picker := &loadbalance.Picker{}
	picker.Build(buildInfo)
	return picker, subConns
}

func getPort(id int) int {
	return 61059 + (id * 2)
}

func getAddr(port int) string {
	addr := fmt.Sprintf("%s:%d", SERVICE_HOST, port)
	return addr
}
