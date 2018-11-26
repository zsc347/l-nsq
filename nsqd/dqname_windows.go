// +build windows

package nsqd

func getBackendName(topicName, channelName string) string {
	return topicName + ";" + channelName
}
