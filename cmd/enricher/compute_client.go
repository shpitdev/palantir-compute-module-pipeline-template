package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/internal/util"
)

type computeModuleJobEnvelope struct {
	ComputeModuleJobV1 computeModuleJobV1 `json:"computeModuleJobV1"`
}

type computeModuleJobV1 struct {
	JobID                         string          `json:"jobId"`
	QueryType                     string          `json:"queryType"`
	Query                         json.RawMessage `json:"query"`
	TemporaryCredentialsAuthToken string          `json:"temporaryCredentialsAuthToken"`
	AuthHeader                    string          `json:"authHeader"`
}

type computeModuleClientConfig struct {
	GetJobURI       string
	PostResultURI   string
	ModuleAuthToken string
	DefaultCAPath   string
}

func loadComputeModuleClientConfigFromEnv() (computeModuleClientConfig, bool, error) {
	getJob := strings.TrimSpace(os.Getenv("GET_JOB_URI"))
	postRes := strings.TrimSpace(os.Getenv("POST_RESULT_URI"))
	if getJob == "" || postRes == "" {
		return computeModuleClientConfig{}, false, nil
	}

	modTok, err := readValueOrFile(strings.TrimSpace(os.Getenv("MODULE_AUTH_TOKEN")), "MODULE_AUTH_TOKEN")
	if err != nil {
		return computeModuleClientConfig{}, false, err
	}
	if strings.TrimSpace(modTok) == "" {
		return computeModuleClientConfig{}, false, fmt.Errorf("MODULE_AUTH_TOKEN is required when GET_JOB_URI/POST_RESULT_URI are set")
	}

	caPath := strings.TrimSpace(os.Getenv("DEFAULT_CA_PATH"))
	if caPath == "" {
		return computeModuleClientConfig{}, false, fmt.Errorf("DEFAULT_CA_PATH is required when GET_JOB_URI/POST_RESULT_URI are set")
	}

	return computeModuleClientConfig{
		GetJobURI:       getJob,
		PostResultURI:   postRes,
		ModuleAuthToken: modTok,
		DefaultCAPath:   caPath,
	}, true, nil
}

func runComputeModuleClientLoop(ctx context.Context, cfg computeModuleClientConfig, handleJob func(context.Context, computeModuleJobV1) ([]byte, error)) error {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	hc, err := newComputeModuleHTTPClient(cfg.DefaultCAPath)
	if err != nil {
		return err
	}

	logger.Printf("compute module client enabled; polling GET_JOB_URI=%s", cfg.GetJobURI)

	sleep := 500 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		job, ok, err := getNextJob(ctx, hc, cfg.GetJobURI, cfg.ModuleAuthToken)
		if err != nil {
			logger.Printf("compute module client: get job failed: %s", util.RedactSecrets(err.Error()))
			time.Sleep(sleep)
			if sleep < 5*time.Second {
				sleep *= 2
			}
			continue
		}
		sleep = 500 * time.Millisecond
		if !ok {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		jobID := strings.TrimSpace(job.JobID)
		if jobID == "" {
			logger.Printf("compute module client: received job without jobId; skipping")
			time.Sleep(500 * time.Millisecond)
			continue
		}

		logger.Printf("compute module client: received jobId=%s queryType=%s", jobID, strings.TrimSpace(job.QueryType))
		result, jobErr := handleJob(ctx, job)
		if jobErr != nil {
			logger.Printf("compute module client: jobId=%s failed: %s", jobID, util.RedactSecrets(jobErr.Error()))
			// Still post a result so the platform can record failure.
			if len(result) == 0 {
				result = []byte(util.RedactSecrets(jobErr.Error()))
			}
		} else if len(result) == 0 {
			result = []byte("ok")
		}

		if err := postResult(ctx, hc, cfg.PostResultURI, cfg.ModuleAuthToken, jobID, result); err != nil {
			logger.Printf("compute module client: post result failed for jobId=%s: %s", jobID, util.RedactSecrets(err.Error()))
			// Retry a few times before moving on.
			for i := 0; i < 5; i++ {
				time.Sleep(time.Duration(i+1) * time.Second)
				if err := postResult(ctx, hc, cfg.PostResultURI, cfg.ModuleAuthToken, jobID, result); err == nil {
					break
				}
			}
		}
	}
}

func newComputeModuleHTTPClient(caPath string) (*http.Client, error) {
	b, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read DEFAULT_CA_PATH: %w", err)
	}
	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(b); !ok {
		return nil, fmt.Errorf("parse DEFAULT_CA_PATH PEM: no certs found")
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}
	return &http.Client{Transport: tr, Timeout: 30 * time.Second}, nil
}

func getNextJob(ctx context.Context, hc *http.Client, getJobURI, moduleAuthToken string) (computeModuleJobV1, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getJobURI, nil)
	if err != nil {
		return computeModuleJobV1{}, false, err
	}
	req.Header.Set("Module-Auth-Token", moduleAuthToken)
	req.Header.Set("Accept", "application/json")

	resp, err := hc.Do(req)
	if err != nil {
		return computeModuleJobV1{}, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNoContent {
		return computeModuleJobV1{}, false, nil
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return computeModuleJobV1{}, false, err
	}
	if resp.StatusCode/100 != 2 {
		return computeModuleJobV1{}, false, fmt.Errorf("GET job: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var env computeModuleJobEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return computeModuleJobV1{}, false, fmt.Errorf("parse GET job response: %w (body=%s)", err, strings.TrimSpace(string(b)))
	}
	return env.ComputeModuleJobV1, true, nil
}

func postResult(ctx context.Context, hc *http.Client, postResultURI, moduleAuthToken, jobID string, result []byte) error {
	base := strings.TrimRight(strings.TrimSpace(postResultURI), "/")
	// Ensure we don't double-encode slashes.
	u := base + "/" + path.Clean("/" + jobID)[1:]

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(result))
	if err != nil {
		return err
	}
	req.Header.Set("Module-Auth-Token", moduleAuthToken)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST result: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}
