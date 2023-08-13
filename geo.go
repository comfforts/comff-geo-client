package geo

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	config "github.com/comfforts/comff-config"
	api "github.com/comfforts/comff-geo/api/v1"
	"github.com/comfforts/logger"

	"github.com/comfforts/comff-geo-client/internal/loadbalance"
)

const DEFAULT_SERVICE_PORT = "62051"
const DEFAULT_SERVICE_HOST = "127.0.0.1"

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

var (
	defaultDialTimeout      = 5 * time.Second
	defaultKeepAlive        = 30 * time.Second
	defaultKeepAliveTimeout = 10 * time.Second
)

const GeoClientContextKey = ContextKey("geo-client")
const DefaultClientName = "comfforts-geo-client"

type ClientOption struct {
	DialTimeout      time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
	Caller           string
}

type Client interface {
	GeoLocate(ctx context.Context, req *api.GeoRequest, opts ...grpc.CallOption) (*api.GeoResponse, error)
	GetGeoRoute(ctx context.Context, req *api.GeoRouteRequest, opts ...grpc.CallOption) (*api.RouteResponse, error)
	GetAddressRoute(ctx context.Context, req *api.AddressRouteRequest, opts ...grpc.CallOption) (*api.RouteResponse, error)
	AddGeo(ctx context.Context, req *api.AddGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationResponse, error)
	GetGeo(ctx context.Context, req *api.GetGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationResponse, error)
	GetGeos(ctx context.Context, req *api.GetGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationsResponse, error)
	DeleteGeo(ctx context.Context, req *api.DeleteGeoLocationRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error)
	AddAddress(ctx context.Context, req *api.AddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error)
	UpdateAddress(ctx context.Context, req *api.AddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error)
	GetAddress(ctx context.Context, req *api.GetAddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error)
	GetAddresses(ctx context.Context, req *api.GetAddressesRequest, opts ...grpc.CallOption) (*api.AddressesResponse, error)
	GetAddressesByIds(ctx context.Context, req *api.GetAddressesRequest, opts ...grpc.CallOption) (*api.AddressesResponse, error)
	DeleteAddress(ctx context.Context, req *api.DeleteAddressRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error)
	GetServers(ctx context.Context, req *api.GetServersRequest, opts ...grpc.CallOption) (*api.GetServersResponse, error)
	Close() error
}

func NewDefaultClientOption() *ClientOption {
	return &ClientOption{
		DialTimeout:      defaultDialTimeout,
		KeepAlive:        defaultKeepAlive,
		KeepAliveTimeout: defaultKeepAliveTimeout,
	}
}

type geoClient struct {
	logger.AppLogger
	client api.GeoClient
	conn   *grpc.ClientConn
	opts   *ClientOption
}

func NewClient(l logger.AppLogger, clientOpts *ClientOption) (*geoClient, error) {
	if clientOpts.Caller == "" {
		clientOpts.Caller = DefaultClientName
	}

	servicePort := os.Getenv("GEO_SERVICE_PORT")
	if servicePort == "" {
		servicePort = DEFAULT_SERVICE_PORT
	}
	serviceHost := os.Getenv("GEO_SERVICE_HOST")
	if serviceHost == "" {
		serviceHost = DEFAULT_SERVICE_HOST
	}

	serviceAddr := fmt.Sprintf("%s:%s", serviceHost, servicePort)
	l.Info("geo client serviceAddr", zap.String("serviceAddr", serviceAddr))
	// with load balancer
	serviceAddr = fmt.Sprintf("%s://%s", loadbalance.GeoCQRSResolverName, serviceAddr)
	l.Info("geo client serviceAddr", zap.String("serviceAddr", serviceAddr))

	tlsConfig, err := config.SetupTLSConfig(&config.ConfigOpts{
		Target: config.GEO_CLIENT,
		Addr:   serviceAddr,
	})
	if err != nil {
		l.Error("error setting geo client TLS", zap.Error(err), zap.String("client", clientOpts.Caller))
		return nil, err
	}
	tlsConfig.InsecureSkipVerify = true
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	conn, err := grpc.Dial(serviceAddr, opts...)
	if err != nil {
		l.Error("geo client failed to connect", zap.Error(err), zap.String("client", clientOpts.Caller))
		return nil, err
	}

	client := api.NewGeoClient(conn)
	l.Info("geo client connected", zap.String("host", serviceHost), zap.String("port", servicePort))
	return &geoClient{
		client:    client,
		AppLogger: l,
		conn:      conn,
		opts:      clientOpts,
	}, nil
}

func (gc *geoClient) GeoLocate(ctx context.Context, req *api.GeoRequest, opts ...grpc.CallOption) (*api.GeoResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GeoLocate(ctx, req)
	if err != nil {
		gc.Error("error geo locating", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetGeoRoute(ctx context.Context, req *api.GeoRouteRequest, opts ...grpc.CallOption) (*api.RouteResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetGeoRoute(ctx, req)
	if err != nil {
		gc.Error("error fetching routes", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetAddressRoute(ctx context.Context, req *api.AddressRouteRequest, opts ...grpc.CallOption) (*api.RouteResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetAddressRoute(ctx, req)
	if err != nil {
		gc.Error("error fetching routes", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) AddGeo(ctx context.Context, req *api.AddGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.AddGeoLocation(ctx, req)
	if err != nil {
		gc.Error("error adding geo location", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetGeo(ctx context.Context, req *api.GetGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetGeoLocation(ctx, req)
	if err != nil {
		gc.Error("error fetching geo location", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetGeos(ctx context.Context, req *api.GetGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationsResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetGeoLocations(ctx, req)
	if err != nil {
		gc.Error("error fetching geo locations", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) DeleteGeo(ctx context.Context, req *api.DeleteGeoLocationRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.DeleteGeoLocation(ctx, req)
	if err != nil {
		gc.Error("error deleting geo location", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) AddAddress(ctx context.Context, req *api.AddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.AddAddress(ctx, req)
	if err != nil {
		gc.Error("error adding address", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) UpdateAddress(ctx context.Context, req *api.AddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.UpdateAddress(ctx, req)
	if err != nil {
		gc.Error("error updating address", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetAddress(ctx context.Context, req *api.GetAddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetAddress(ctx, req)
	if err != nil {
		gc.Error("error fetching address", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetAddresses(ctx context.Context, req *api.GetAddressesRequest, opts ...grpc.CallOption) (*api.AddressesResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetAddresses(ctx, req)
	if err != nil {
		gc.Error("error fetching addresses", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetAddressesByIds(ctx context.Context, req *api.GetAddressesRequest, opts ...grpc.CallOption) (*api.AddressesResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetAddressesByIds(ctx, req)
	if err != nil {
		gc.Error("error fetching addresses", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) DeleteAddress(ctx context.Context, req *api.DeleteAddressRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.DeleteAddress(ctx, req)
	if err != nil {
		gc.Error("error deleting address", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetServers(ctx context.Context, req *api.GetServersRequest, opts ...grpc.CallOption) (*api.GetServersResponse, error) {
	ctx, cancel := gc.contextWithOptions(ctx, gc.opts)
	defer cancel()

	resp, err := gc.client.GetServers(ctx, req)
	if err != nil {
		gc.Error("error getting server list", zap.Error(err), zap.String("client", gc.opts.Caller))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) Close() error {
	if err := gc.conn.Close(); err != nil {
		gc.Error("error closing geo client connection", zap.Error(err), zap.String("client", gc.opts.Caller))
		return err
	}
	return nil
}

func (gc *geoClient) contextWithOptions(ctx context.Context, opts *ClientOption) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, gc.opts.DialTimeout)
	if gc.opts.Caller != "" {
		md := metadata.New(map[string]string{"service-client": gc.opts.Caller})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	return ctx, cancel
}
