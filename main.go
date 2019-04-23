package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"cloud.google.com/go/pubsub"
	"github.com/alecthomas/kingpin"
	retry "github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/zpages"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type proxy struct {
	endpoint               *url.URL
	client                 *pubsub.Client
	subscription           string
	maxExtension           time.Duration
	maxOutstandingMessages int
	httpClient             *retry.Client
	logger                 *zap.Logger
}

type pushMessage struct {
	Attributes map[string]string `json:"attributes"`
	Data       []byte            `json:"data"`
	ID         string            `json:"message_id"`
}

type pushRequest struct {
	Message      pushMessage
	Subscription string `json:"subscription"`
}

var version = "unknown"

func main() {
	project := kingpin.Flag("project", "GCP project").Envar("GCP_PROJECT").Short('p').Required().String()
	lvl := newLogLevel(kingpin.Flag("log-level", "log level: valid options are debug, info, warn, and error").Short('l').PlaceHolder("LOGLEVEL"))
	endpoint := kingpin.Flag("endpoint", "URL to POST pubsub message").Short('e').Required().String()
	maxExtension := kingpin.Flag("max-extension", "maximum period for which the Subscription should automatically extend the ack deadline for each message").Short('m').Default("5m").Duration()
	maxOutstandingMessages := kingpin.Flag("max-outstanding-messages", "maximum number of unprocessed messages (unacknowledged but not yet expired)").Short('o').Default("8").Int()
	subscription := kingpin.Flag("subscription", "name of the PubSub subscription").Envar("SUBSCRIPTION").Short('s').Required().String()
	retries := kingpin.Flag("retries", "number of times to retry an HTTP POST").Short('r').Default("10").Int()
	addr := kingpin.Flag("addr", "listen address for metrics handler").Short('a').Default("127.0.0.1:8080").String()
	kingpin.Version(version)

	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	logger, err := newLogger(lvl.Level)
	if err != nil {
		panic(err)
	}

	pe, err := prometheus.NewExporter(prometheus.Options{})
	if err != nil {
		logger.Fatal("failed to create prometheus metrics exporter", zap.Error(err))
	}

	view.RegisterExporter(pe)
	if err := view.Register(ochttp.ClientRoundtripLatencyDistribution); err != nil {
		panic(err)
	}

	view.SetReportingPeriod(time.Second)

	u, err := url.Parse(*endpoint)
	if err != nil {
		logger.Fatal("failed to parse endpoint", zap.Error(err))
	}

	p := &proxy{
		endpoint:               u,
		maxExtension:           *maxExtension,
		maxOutstandingMessages: *maxOutstandingMessages,
		subscription:           *subscription,
		httpClient:             newHTTPClient(logger, *retries),
		logger:                 logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := pubsub.NewClient(ctx, *project)

	if err != nil {
		logger.Fatal("failed to create pubsub client", zap.Error(err))
	}

	p.client = c

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := p.Run(ctx); err != nil {
			logger.Error("failed to run proxy", zap.Error(err))
			return err
		}
		return nil
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", pe)
	zpages.Handle(mux, "/debug")

	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		wait, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		if err := srv.Shutdown(wait); err != nil {
			logger.Error("HTTP server shutdown", zap.Error(err))
		}
	}()

	g.Go(func() error {
		return srv.ListenAndServe()
	})

	if err := g.Wait(); err == nil {
		log.Fatal("failed to run", zap.Error(err))
	}
}

func (p *proxy) Run(ctx context.Context) error {
	subscription := p.client.Subscription(p.subscription)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to check if subscription exists %s", p.subscription)
	}
	if !exists {
		return errors.Errorf("subscription does not exist %s", p.subscription)
	}

	subscription.ReceiveSettings.MaxExtension = p.maxExtension
	subscription.ReceiveSettings.MaxOutstandingMessages = p.maxOutstandingMessages
	subscription.ReceiveSettings.Synchronous = true

	f := func(ctx context.Context, msg *pubsub.Message) {
		if err := p.handleMessage(ctx, msg); err != nil {
			msg.Nack()
			if errors.Cause(err) != context.Canceled {
				p.logger.Error("failed to handle message", zap.Error(errors.Cause(err)))
			}
			return
		}
		msg.Ack()
	}
	if err := subscription.Receive(ctx, f); err != nil && errors.Cause(err) != context.Canceled {
		p.logger.Error("failed to receive message", zap.Error(err))
	}

	return nil
}

func (p *proxy) handleMessage(ctx context.Context, msg *pubsub.Message) error {

	pr := pushRequest{
		Message: pushMessage{
			Attributes: msg.Attributes,
			Data:       msg.Data,
			ID:         msg.ID,
		},
		Subscription: p.subscription,
	}

	data, err := json.Marshal(&pr)
	if err != nil {
		return errors.Wrap(err, "failed to marshal message")
	}

	req := &retry.Request{
		Request: &http.Request{
			Method:     http.MethodPost,
			URL:        p.endpoint,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Body:       ioutil.NopCloser(bytes.NewBuffer(data)),
			Host:       p.endpoint.Host,
		},
	}

	req.Header.Set("Content-Type", "application/json")

	req = req.WithContext(ctx)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "http request failed")
	}
	defer resp.Body.Close()

	return nil
}

type logLevel struct {
	zapcore.Level
}

func newLogLevel(s kingpin.Settings) *logLevel {
	l := &logLevel{}
	s.SetValue(l)
	return l
}

func (l *logLevel) Set(value string) error {
	return l.Level.Set(value)
}

func (l *logLevel) Get() interface{} {
	return l
}

func (l *logLevel) String() string {
	return l.Level.String()
}

func newLogger(lvl zapcore.Level) (*zap.Logger, error) {

	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	a := zap.NewAtomicLevel()
	a.SetLevel(lvl)

	config := zap.Config{
		Development:       false,
		DisableCaller:     true,
		DisableStacktrace: true,
		EncoderConfig:     cfg,
		Encoding:          "json",
		ErrorOutputPaths:  []string{"stderr"},
		Level:             a,
		OutputPaths:       []string{"stdout"},
	}
	l, err := config.Build()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create logger")
	}
	return l, nil
}

type logWrapper struct {
	*zap.SugaredLogger
}

func (l *logWrapper) Printf(format string, args ...interface{}) {

	parts := strings.SplitN(format, " ", 2)
	if len(parts) != 2 {
		l.Errorf(format, args...)
	}

	switch parts[0] {
	case "[DEBUG]":
		l.Debugf(parts[1], args...)
	default:
		l.Errorf(parts[1], args...)
	}

}

func newHTTPClient(logger *zap.Logger, retries int) *retry.Client {
	return &retry.Client{
		HTTPClient:   &http.Client{Transport: &ochttp.Transport{}},
		Logger:       &logWrapper{logger.Sugar()},
		RetryWaitMin: time.Millisecond * 500,
		RetryWaitMax: time.Second * 10,
		RetryMax:     retries,
		CheckRetry:   retryPolicy,
		Backoff:      retry.DefaultBackoff,
		ErrorHandler: errorHandler,
	}

}

// see https://github.com/hashicorp/go-retryablehttp/blob/4ea34a9a437e0e910fe5fea5be81372a39336bb0/client.go#L292
func retryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	rc, err := retry.DefaultRetryPolicy(ctx, resp, err)

	if rc || err != nil {
		return rc, err
	}

	// retry on all non-200's
	return resp.StatusCode != 200, nil
}

func errorHandler(resp *http.Response, err error, numTries int) (*http.Response, error) {
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return resp, errors.Wrap(err, "HTTP request failed")
	}

	return resp, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
}
