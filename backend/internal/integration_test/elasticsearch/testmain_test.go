package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	"log"
	"os"
	"testing"
)

var es *elasticsearch.Client
var logger, _ = zap.NewDevelopment()

const port = "9201"
const dbTestContainerName = "test_elasticsearch"

func TestMain(m *testing.M) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	ctx := context.Background()
	uri, cleanup, err := startElasticSearchContainer(
		ctx,
		dbTestContainerName,
		port,
		logger,
	)
	defer cleanup()
	if err != nil {
		logger.Fatal("Failed to start container", zap.Error(err))
	}
	es, err = elasticsearch.NewClient(
		elasticsearch.Config{
			Addresses: []string{uri},
		},
	)
	if err != nil {
		logger.Fatal("Failed to create elasticsearch client", zap.Error(err))
	}
	info, err := es.Info()
	if err != nil {
		logger.Fatal("Failed to get elasticsearch info", zap.Error(err))
	}
	log.Printf("Elasticsearch Info: %v", info)

	bs := bootstrapper.NewBootstrapper(es, logger)
	err = bs.BootstrapElasticsearch()
	if err != nil {
		logger.Error("Failed to bootstrap elasticsearch", zap.Error(err))
	}
	code := m.Run()
	os.Exit(code)
}
