package redis

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/log"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly/pkg/sharedstorage"
)

type Redis struct {
}

type Factory struct {
}

func (f *Factory) Type() string {
	return "redis"
}

func (f *Factory) NewInstance() sharedstorage.Plugin {
	//create a new instance and return its address
	return &Redis{}
}

func (f *Factory) InitConfig(config config.Section) {
}

func (r *Redis) Init(ctx context.Context, config config.Section) error {
	return nil
}

func (r *Redis) Name() string { return "redis" }

// Init initializes the plugin, with configuration

// SetHandler registers a handler to receive callbacks
// Plugin will attempt (but is not guaranteed) to deliver events only for the given namespace
func (r *Redis) SetHandler(namespace string, handler sharedstorage.Callbacks) {

}

// Capabilities returns capabilities - not called until after Init
func (r *Redis) Capabilities() *sharedstorage.Capabilities {
	return &sharedstorage.Capabilities{}
}

// UploadData publishes data to the Shared Storage, and returns a payload reference ID
func (r *Redis) UploadData(ctx context.Context, data io.Reader) (payloadRef string, err error) {
	log.L(ctx).Info("Redis:UploadData")

	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	key := uuid.New()
	payloadRef = key.String()
	buf := new(bytes.Buffer)
	buf.ReadFrom(data)
	payload := buf.String()

	log.L(ctx).Infof("Redis:UploadData key=%s", payloadRef)
	log.L(ctx).Infof("Redis:UploadData payload=%s", payload)

	err = rdb.Set(ctx, payloadRef, payload, 0).Err()
	if err != nil {
		panic(err)
	}
	return payloadRef, nil

}

func (r *Redis) DownloadData(ctx context.Context, payloadRef string) (data io.ReadCloser, err error) {
	log.L(ctx).Infof("Redis:DownloadData key=%s", payloadRef)

	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	val, err := rdb.Get(ctx, payloadRef).Result()
	if err != nil {
		panic(err)
	}
	log.L(ctx).Infof("Redis:DownloadData val=%s", val)

	return io.NopCloser(strings.NewReader(val)), nil
}
