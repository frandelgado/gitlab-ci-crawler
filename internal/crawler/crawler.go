package crawler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/catouc/gitlab-ci-crawler/internal/gitlab"
	"github.com/catouc/gitlab-ci-crawler/internal/storage"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
)

const gitlabCIFileName = ".gitlab-ci.yml"

type Crawler struct {
	config       *Config
	gitlabClient *gitlab.Client
	storage      storage.Storage
	logger       zerolog.Logger

	projectSetMut sync.RWMutex
	projectSet    map[string]struct{}
}

// New creates a new project crawler
// The caller is responsible for closing the neo4j driver and session
// the Crawl func handles this already.
func New(cfg *Config, logger zerolog.Logger, store storage.Storage) (*Crawler, error) {
	retryClient := retryablehttp.NewClient()

	retryClient.RetryMax = cfg.HTTPClientMaxRetry
	retryClient.RetryWaitMax = cfg.HTTPClientMaxRetryWait
	retryClient.RetryWaitMin = cfg.HTTPClientMinRetryWait
	retryClient.HTTPClient = &http.Client{Timeout: cfg.HTTPClientTimeout}

	httpClient := &rateLimitedHTTPClient{
		Client:      retryClient.StandardClient(),
		RateLimiter: rate.NewLimiter(rate.Limit(cfg.GitlabMaxRPS), cfg.GitlabMaxRPS),
	}

	gitlabClient := gitlab.NewClient(cfg.GitlabHost, cfg.GitlabToken, httpClient, logger)

	return &Crawler{
		config:       cfg,
		gitlabClient: gitlabClient,
		storage:      store,
		logger:       logger,
		projectSet:   make(map[string]struct{}),
	}, nil
}

// Crawl iterates through every project in the given GitLab host
// and parses the CI file, and it's includes into the given Neo4j instance
func (c *Crawler) Crawl(ctx context.Context) error {

	c.logger.Info().Msg("Starting to crawl...")
	resultChan := make(chan gitlab.Project, 200)

	var streamOK bool

	go func() {
		defer close(resultChan)

		if err := c.gitlabClient.StreamAllProjects(ctx, 100, resultChan); err != nil {
			c.logger.Err(err).Msg("stopping crawler: error in project stream")
			return
		}

		streamOK = true
	}()

	dependencyProjects := make(chan string, 100)
	for i := 0; i < 30; i++ {
		go c.worker(ctx, resultChan, dependencyProjects)
	}
	err := c.writeDependenciesToFile(dependencyProjects)
	if err != nil {
		return err
	}

	if !streamOK {
		return errors.New("stream failed")
	}

	c.logger.Info().Msg("stopped crawling")
	return nil
}

func (c *Crawler) worker(ctx context.Context, projects chan gitlab.Project, dependencyProjects chan string) {
	for p := range projects {

		c.projectSetMut.Lock()
		_, exists := c.projectSet[p.PathWithNamespace]
		if exists {
			c.projectSetMut.Unlock()
			continue
		}
		c.projectSet[p.PathWithNamespace] = struct{}{}
		c.projectSetMut.Unlock()

		if err := c.updateProjectInGraph(ctx, p, dependencyProjects); err != nil {
			c.logger.Err(err).
				Str("ProjectPath", p.PathWithNamespace).
				Int("ProjectID", p.ID).
				Msg("failed to parse project")
		}
	}

}

func (c *Crawler) writeDependenciesToFile(dependencyProjects chan string) error {

	filename := "/Users/fdelgado2/dependency-projects-2"
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return errors.New("could not open file " + filename)
	}
	defer f.Close()

	for p := range dependencyProjects {
		if _, err = f.WriteString(p + "\n"); err != nil {
			return errors.New("could not append to file")
		}
	}
	return nil
}

func (c *Crawler) updateProjectInGraph(ctx context.Context, project gitlab.Project, dependencyProjects chan string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:

		if len(project.DefaultBranch) == 0 {
			c.logger.Debug().
				Str("Project", project.PathWithNamespace).
				Msg("Project has no DefaultBranch")

			return nil
		}

		err := c.handleIncludes(ctx, project, dependencyProjects, gitlabCIFileName)
		if err != nil {
			c.logger.Error().
				Err(err).
				Str("Project", project.PathWithNamespace).
				Msg("failed to handle all includes")
		}
		return nil
	}
}

func (c *Crawler) handleIncludes(ctx context.Context, project gitlab.Project, dependencyProjects chan string, filePath string) error {
	gitlabCIFile, err := c.gitlabClient.GetRawFileFromProject(ctx, project.ID, filePath, project.DefaultBranch)
	if err != nil {
		if errors.Is(err, gitlab.ErrRawFileNotFound) {
			return nil
		}
		return fmt.Errorf("failed to get file %s: %w", filePath, err)
	}

	includes, err := c.parseIncludes(gitlabCIFile)
	if err != nil {
		return fmt.Errorf("failed to parse includes: %w", err)
	}

	includes = c.enrichIncludes(includes, project, c.config.DefaultRefName)

	for _, i := range includes {
		if i.Ref == "" {
			c.logger.Warn().
				Str("Project", i.Project).
				Str("Files", strings.Join(i.Files, ",")).
				Msg("Got empty ref")
		}
		if i.Project != "" {
			dependencyProjects <- i.Project
		}
	}

	return nil
}
