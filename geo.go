package geo

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/comfforts/comff-geo/api/v1"
	"github.com/comfforts/logger"

	"github.com/comfforts/comff-geo-client/internal/config"
)

const SERVICE_PORT = 54051
const SERVICE_DOMAIN = "127.0.0.1"

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

type ClientOption struct {
	DialTimeout      time.Duration
	KeepAlive        time.Duration
	KeepAliveTimeout time.Duration
}

type Client interface {
	GeoLocate(ctx context.Context, req *api.GeoRequest, opts ...grpc.CallOption) (*api.GeoResponse, error)
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
	logger logger.AppLogger
	client api.GeoClient
	conn   *grpc.ClientConn
}

func NewClient(logger logger.AppLogger, clientOpts *ClientOption) (*geoClient, error) {
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.CertFile(config.GeoClientCertFile),
		KeyFile:  config.CertFile(config.GeoClientKeyFile),
		CAFile:   config.CertFile(config.CAFile),
		Server:   false,
	})
	if err != nil {
		logger.Error("error setting geo client TLS", zap.Error(err))
		return nil, err
	}
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}

	serviceAddr := fmt.Sprintf("%s:%d", SERVICE_DOMAIN, SERVICE_PORT)

	conn, err := grpc.Dial(serviceAddr, opts...)
	if err != nil {
		logger.Error("client failed to connect", zap.Error(err))
		return nil, err
	}

	client := api.NewGeoClient(conn)

	return &geoClient{
		client: client,
		logger: logger,
		conn:   conn,
	}, nil
}

func (gc *geoClient) GeoLocate(ctx context.Context, req *api.GeoRequest, opts ...grpc.CallOption) (*api.GeoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.GeoLocate(ctx, req)
	if err != nil {
		gc.logger.Error("error geo locating", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) AddGeo(ctx context.Context, req *api.AddGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.AddGeoLocation(ctx, req)
	if err != nil {
		gc.logger.Error("error adding geo location", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetGeo(ctx context.Context, req *api.GetGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.GetGeoLocation(ctx, req)
	if err != nil {
		gc.logger.Error("error fetching geo location", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetGeos(ctx context.Context, req *api.GetGeoLocationRequest, opts ...grpc.CallOption) (*api.GeoLocationsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.GetGeoLocations(ctx, req)
	if err != nil {
		gc.logger.Error("error fetching geo locations", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) DeleteGeo(ctx context.Context, req *api.DeleteGeoLocationRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.DeleteGeoLocation(ctx, req)
	if err != nil {
		gc.logger.Error("error deleting geo location", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) AddAddress(ctx context.Context, req *api.AddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.AddAddress(ctx, req)
	if err != nil {
		gc.logger.Error("error adding address", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) UpdateAddress(ctx context.Context, req *api.AddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.UpdateAddress(ctx, req)
	if err != nil {
		gc.logger.Error("error updating address", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetAddress(ctx context.Context, req *api.GetAddressRequest, opts ...grpc.CallOption) (*api.AddressResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.GetAddress(ctx, req)
	if err != nil {
		gc.logger.Error("error fetching address", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetAddresses(ctx context.Context, req *api.GetAddressesRequest, opts ...grpc.CallOption) (*api.AddressesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.GetAddresses(ctx, req)
	if err != nil {
		gc.logger.Error("error fetching addresses", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) GetAddressesByIds(ctx context.Context, req *api.GetAddressesRequest, opts ...grpc.CallOption) (*api.AddressesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.GetAddressesByIds(ctx, req)
	if err != nil {
		gc.logger.Error("error fetching addresses", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) DeleteAddress(ctx context.Context, req *api.DeleteAddressRequest, opts ...grpc.CallOption) (*api.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := gc.client.DeleteAddress(ctx, req)
	if err != nil {
		gc.logger.Error("error deleting address", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (gc *geoClient) Close() error {
	if err := gc.conn.Close(); err != nil {
		gc.logger.Error("error closing shop client connection", zap.Error(err))
		return err
	}
	return nil
}
