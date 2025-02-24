package elasticsearch

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"time"
)

const elasticSearchPort = "9200"

func startElasticSearchContainer(
	ctx context.Context,
	containerName string,
	port string,
	logger *zap.Logger,
) (
	elasticSearchURI string,
	stopContainer func(),
	err error,
) {
	childCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	newNetwork, err := network.New(childCtx)
	if err != nil {
		logger.Fatal("Error while creating network", zap.Error(err))
	}
	networkName := newNetwork.Name
	logger.Info("Network Name", zap.String("networkName", networkName))

	req := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/elasticsearch/elasticsearch:8.10.2",
		Name:         containerName,
		ExposedPorts: []string{fmt.Sprintf("%s:%s", port, elasticSearchPort)},
		WaitingFor:   wait.ForListeningPort(elasticSearchPort),
		Networks:     []string{networkName},
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
		},
	}

	elasticSearchContainer, err := testcontainers.GenericContainer(childCtx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return "", nil, fmt.Errorf("failed to start container: %w", err)
	}

	stopContainer = func() {
		elasticSearchContainer.Terminate(childCtx)
	}

	// Get the container IP
	host, err := elasticSearchContainer.Host(childCtx)
	if err != nil {
		stopContainer()
		return "", nil, fmt.Errorf("failed to get container host: %w", err)
	}

	// Get the mapped port
	p, err := elasticSearchContainer.MappedPort(childCtx, elasticSearchPort)
	if err != nil {
		stopContainer()
		return "", nil, fmt.Errorf("failed to get container port: %w", err)
	}

	elasticSearchURI = fmt.Sprintf("http://%s:%s", host, p.Port())
	logger.Info("Elasticsearch URI", zap.String("elasticSearchURI", elasticSearchURI))
	return elasticSearchURI, stopContainer, nil
}
