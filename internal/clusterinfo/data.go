package clusterinfo

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/l-nsq/internal/stringy"

	"github.com/l-nsq/internal/http_api"
	"github.com/l-nsq/internal/lg"
)

// ErrList define a list of errors
type ErrList []error

func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

// Errors return all errors in error list
func (l ErrList) Errors() []error {
	return l
}

// ClusterInfo define struct to get cluster information
type ClusterInfo struct {
	log    lg.AppLogFunc
	client *http_api.Client
}

// New create a new cluster
func New(log lg.AppLogFunc, client *http_api.Client) *ClusterInfo {
	return &ClusterInfo{
		log:    log,
		client: client,
	}
}

// GetLookupdTopicChannels returns a []string containing a union of all the channels
// from all the given lookupd for the given topic
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	var channels []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	var errs []error

	type respType struct {
		Channels []string `json:"channels"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
			c.logf("CI: querying nsqlookupd %s", endpoint)

			var resp respType
			err := c.client.GETV1(endpoint, &resp)
			if err != nil {
				lock.Lock()
				errs = append(errs, err)
				lock.Unlock()
				return
			}

			lock.Lock()
			defer lock.Unlock()
			channels = append(channels, resp.Channels...)
		}(addr)
	}
	wg.Wait()

	if len(errs) == len(lookupdHTTPAddrs) {
		return nil, fmt.Errorf("Failed to query any nsqlookupd: %s", ErrList(errs))
	}

	channels = stringy.Uniq(channels)
	sort.Strings(channels)

	if len(errs) > 0 {
		return channels, ErrList(errs)
	}
	return channels, nil
}

func (c *ClusterInfo) logf(f string, args ...interface{}) {
	if c.log != nil {
		c.log(lg.INFO, f, args)
	}
}
