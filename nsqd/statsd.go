package nsqd

import (
	"fmt"
	"math"
	"net"
	"time"

	"github.com/l-nsq/internal/lg"
	"github.com/l-nsq/internal/statsd"
	"github.com/l-nsq/internal/writers"
)

// Uint64Slice for sort uint64 slice
type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Less(i, j int) bool {
	return (s)[i] < (s)[j]
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func percentile(perc float64, arr []uint64, length int) uint64 {
	if length == 0 {
		return 0
	}
	indexOfPerc := int(math.Floor((perc/100.0)*float64(length) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

func (n *NSQD) statsdLoop() {
	var lastMemStats memStats
	var lastStats []TopicStats
	interval := n.getOpts().StatsdInterval
	ticker := time.NewTicker(interval)

	for {
		select {
		case <-n.exitChan:
			goto exit
		case <-ticker.C:
			addr := n.getOpts().StatsdAddress
			conn, err := net.DialTimeout("udp", addr, time.Second)
			if err != nil {
				n.logf(lg.ERROR, "failed to create UDP socket to statsd(%s)", addr)
				continue
			}

			sw := writers.NewSpreadWriter(conn, interval-time.Second, n.exitChan)
			bw := writers.NewBoundaryBufferedWriter(sw, n.getOpts().StatsdUDPPacketSize)

			prefix := n.getOpts().StatsdPrefix
			client := statsd.NewClient(bw, prefix)
			n.logf(lg.INFO, "STATSD: pushing stats to %s", addr)

			stats := n.GetStats("", "")
			for _, topic := range stats {
				// try to find the topic in the last collection
				lastTopic := TopicStats{}
				for _, checkTopic := range lastStats {
					if topic.TopicName == checkTopic.TopicName {
						lastTopic = checkTopic
						break
					}
				}
				diff := topic.MessageCount - lastTopic.MessageCount
				stat := fmt.Sprintf("topic.%s.message_count", topic.TopicName)
				client.Incr(stat, int64(diff))

				stat = fmt.Sprintf("topic.%s.depth", topic.TopicName)
				client.Guage(stat, topic.Depth)

				stat = fmt.Sprintf("topic.%s.backend_depth", topic.TopicName)
				client.Guage(stat, topic.BackendDepth)

				for _, item := range topic.E2eProcessingLatency.Percentiles {
					stat = fmt.Sprintf("topic.%s.e2e_processing_latency_%.0f",
						topic.TopicName, item["quantile"]*100.0)
					// We can cast the value to int64 since a value of 1 is the
					// minimum resolution we will have, so there is no loss of
					// accuracy
					client.Guage(stat, int64(item["value"]))
				}

				for _, channel := range topic.Channels {
					// try to find the channel in the last collection
					lastChannel := ChannelStats{}
					for _, checkChannel := range lastTopic.Channels {
						if channel.ChannelName == checkChannel.ChannelName {
							lastChannel = checkChannel
							break
						}
					}
					diff := channel.MessageCount - lastChannel.MessageCount
					stat := fmt.Sprintf("topic.%s.channel.%s.message_count",
						topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.depth",
						topic.TopicName, channel.ChannelName)
					client.Guage(stat, channel.Depth)

					stat = fmt.Sprintf("topic.%s.channel.%s.backend_depth",
						topic.TopicName, channel.ChannelName)
					client.Guage(stat, channel.BackendDepth)

					stat = fmt.Sprintf("topic.%s.channe.%s.in_flight_count",
						topic.TopicName, channel.ChannelName)
					client.Guage(stat, int64(channel.InFlightCount))

					stat = fmt.Sprintf("topic.%s.channe.%s.deferred_count",
						topic.TopicName, channel.ChannelName)
					client.Guage(stat, int64(channel.DeferredCount))

					diff = channel.RequeueCount - lastChannel.RequeueCount
					stat = fmt.Sprintf("topic.%s.channe.%s.requeque_count",
						topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(channel.RequeueCount))

					diff = channel.TimeoutCount - lastChannel.TimeoutCount
					stat = fmt.Sprintf("topic.%s.channe.%s.timeout_count",
						topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(channel.TimeoutCount))

					stat = fmt.Sprintf("topic.%s.channel.%s.clients",
						topic.TopicName, channel.ChannelName)
					client.Guage(stat, int64(len(channel.Clients)))

					for _, item := range channel.E2eProcessingLatency.Percentiles {
						stat = fmt.Sprintf("topic.%s.channel.%s.e2e_processing_latency_%.0f",
							topic.TopicName, channel.ChannelName)
						client.Guage(stat, int64(item["value"]))
					}
				}
			}
			lastStats = stats

			if n.getOpts().StatsdMemStats {
				ms := getMemStats()

				client.Guage("mem.heap_objects", int64(ms.HeapObjects))
				client.Guage("mem.heap_idle_bytes", int64(ms.HeapIdleBytes))
				client.Guage("mem.heap_in_use_bytes", int64(ms.HeapInUseBytes))
				client.Guage("mem.heap_released_byte", int64(ms.HeapReleasedBytes))
				client.Guage("mem.gc_pause_usec_100", int64(ms.GCPauseUsec100))
				client.Guage("mem.gc_pause_usec_99", int64(ms.GCPauseUsec99))
				client.Guage("mem.gc_pause_usec_95", int64(ms.GCPauseUsec95))
				client.Guage("mem.next_gc_bytes", int64(ms.NextGCBytes))
				client.Incr("mem.gc_runs", int64(ms.GCTotalRuns-lastMemStats.GCTotalRuns))

				lastMemStats = ms
			}

			bw.Flush()
			sw.Flush()
			conn.Close()
		}
	}

exit:
	ticker.Stop()
	n.logf(lg.INFO, "STATSD: closing")
}
