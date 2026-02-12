package keepalive

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
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/palantir/palantir-compute-module-pipeline-search/pkg/pipeline/redact"
)

type computeModuleJobEnvelope struct {
	ComputeModuleJobV1 Job `json:"computeModuleJobV1"`
}

// Job represents one internal compute-module job from Foundry runtime endpoints.
type Job struct {
	JobID                         string          `json:"jobId"`
	QueryType                     string          `json:"queryType"`
	Query                         json.RawMessage `json:"query"`
	TemporaryCredentialsAuthToken string          `json:"temporaryCredentialsAuthToken"`
	AuthHeader                    string          `json:"authHeader"`
}

// Config controls compute-module keepalive polling.
type Config struct {
	GetJobURI       string
	PostResultURI   string
	ModuleAuthToken string
	DefaultCAPath   string
}

func LoadConfigFromEnv() (Config, bool, error) {
	getJob, err := normalizeLocalhostURI(strings.TrimSpace(os.Getenv("GET_JOB_URI")))
	if err != nil {
		return Config{}, false, fmt.Errorf("invalid GET_JOB_URI: %w", err)
	}
	postRes, err := normalizeLocalhostURI(strings.TrimSpace(os.Getenv("POST_RESULT_URI")))
	if err != nil {
		return Config{}, false, fmt.Errorf("invalid POST_RESULT_URI: %w", err)
	}
	if getJob == "" || postRes == "" {
		return Config{}, false, nil
	}

	modTok, err := readValueOrFile(strings.TrimSpace(os.Getenv("MODULE_AUTH_TOKEN")), "MODULE_AUTH_TOKEN")
	if err != nil {
		return Config{}, false, err
	}
	if strings.TrimSpace(modTok) == "" {
		return Config{}, false, fmt.Errorf("MODULE_AUTH_TOKEN is required when GET_JOB_URI/POST_RESULT_URI are set")
	}

	caPath := strings.TrimSpace(os.Getenv("DEFAULT_CA_PATH"))
	if caPath == "" {
		return Config{}, false, fmt.Errorf("DEFAULT_CA_PATH is required when GET_JOB_URI/POST_RESULT_URI are set")
	}

	return Config{
		GetJobURI:       getJob,
		PostResultURI:   postRes,
		ModuleAuthToken: modTok,
		DefaultCAPath:   caPath,
	}, true, nil
}

func normalizeLocalhostURI(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	// Foundry commonly injects localhost URIs. Go may resolve "localhost" to ::1 first,
	// but the runtime sidecar often binds only to IPv4 loopback. Force IPv4 to avoid
	// flapping with connection refused.
	host := strings.TrimSpace(u.Hostname())
	if host == "localhost" || host == "::1" {
		port := strings.TrimSpace(u.Port())
		if port != "" {
			u.Host = "127.0.0.1:" + port
		} else {
			u.Host = "127.0.0.1"
		}
	}
	return u.String(), nil
}

// RunLoop polls Foundry internal module endpoints and acknowledges jobs.
func RunLoop(ctx context.Context, cfg Config, handleJob func(context.Context, Job) ([]byte, error)) error {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	hc, err := newHTTPClient(cfg.DefaultCAPath)
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
			logger.Printf("compute module client: get job failed: %s", redact.Secrets(err.Error()))
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
			logger.Printf("compute module client: jobId=%s failed: %s", jobID, redact.Secrets(jobErr.Error()))
			if len(result) == 0 {
				result = []byte(redact.Secrets(jobErr.Error()))
			}
		} else if len(result) == 0 {
			result = []byte("ok")
		}

		if err := postResult(ctx, hc, cfg.PostResultURI, cfg.ModuleAuthToken, jobID, result); err != nil {
			logger.Printf("compute module client: post result failed for jobId=%s: %s", jobID, redact.Secrets(err.Error()))
			for i := 0; i < 5; i++ {
				time.Sleep(time.Duration(i+1) * time.Second)
				if err := postResult(ctx, hc, cfg.PostResultURI, cfg.ModuleAuthToken, jobID, result); err == nil {
					break
				}
			}
		}
	}
}

func newHTTPClient(caPath string) (*http.Client, error) {
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

func getNextJob(ctx context.Context, hc *http.Client, getJobURI, moduleAuthToken string) (Job, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getJobURI, nil)
	if err != nil {
		return Job{}, false, err
	}
	req.Header.Set("Module-Auth-Token", moduleAuthToken)
	req.Header.Set("Accept", "application/json")

	resp, err := hc.Do(req)
	if err != nil {
		return Job{}, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNoContent {
		return Job{}, false, nil
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return Job{}, false, err
	}
	if resp.StatusCode/100 != 2 {
		return Job{}, false, fmt.Errorf("GET job: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var env computeModuleJobEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return Job{}, false, fmt.Errorf("parse GET job response: %w (body=%s)", err, strings.TrimSpace(string(b)))
	}
	return env.ComputeModuleJobV1, true, nil
}

func postResult(ctx context.Context, hc *http.Client, postResultURI, moduleAuthToken, jobID string, result []byte) error {
	base := strings.TrimRight(strings.TrimSpace(postResultURI), "/")
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

func readValueOrFile(v string, varName string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", nil
	}
	if strings.Contains(v, "\n") || strings.Contains(v, "\r") {
		return strings.TrimSpace(v), nil
	}
	if fi, err := os.Stat(v); err == nil && !fi.IsDir() {
		b, err := os.ReadFile(v)
		if err != nil {
			return "", fmt.Errorf("read %s file: %w", varName, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return v, nil
}
