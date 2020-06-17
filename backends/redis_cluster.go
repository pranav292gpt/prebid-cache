package backends

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prebid/prebid-cache/config"
	log "github.com/sirupsen/logrus"
)

type RedisCluster struct {
	cfg    config.RedisCluster
	client *redis.ClusterClient
}

func NewRedisClusterBackend(cfg config.RedisCluster) *RedisCluster {

	options := &redis.ClusterOptions{
		Addrs:     cfg.Hosts,
		//Password: cfg.ClusterPassword
	}

	client := redis.NewClusterClient(options)

	var ctx = context.Background()
	_, err := client.Ping(ctx).Result()


	if err != nil {
		log.Fatalf("Error creating RedisCluster backend: %v", err)
	}

	log.Infof("Connected to Redis Cluster")

	return &RedisCluster{
		cfg:    cfg,
		client: client,
	}
}

func (redis *RedisCluster) Get(ctx context.Context, key string) (string, error) {
	res, err := redis.client.Get(ctx,key).Result()

	if err != nil {
		return "", err
	}

	return string(res), nil
}

func (redis *RedisCluster) Put(ctx context.Context, key string, value string, ttlSeconds int) error {
	if ttlSeconds == 0 {
		ttlSeconds = redis.cfg.Expiration * 60
	}
	err := redis.client.Set(ctx, key, value, time.Duration(ttlSeconds)*time.Second).Err()

	if err != nil {
		return err
	}

	return nil
}