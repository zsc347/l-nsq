package protocol

import (
	"fmt"
	"testing"

	"github.com/l-nsq/internal/test"
)

func TestValidName(t *testing.T) {
	test.Equal(t, true, IsValidTopicName("a"))
	test.Equal(t, true, IsValidTopicName("a1"))
	test.Equal(t, true, IsValidTopicName("1"))
	test.Equal(t, true, IsValidTopicName("1a"))
	test.Equal(t, true, IsValidTopicName("1A"))
	test.Equal(t, true, IsValidTopicName("1A#ephemeral"))
}

func TestInvalidName(t *testing.T) {
	fmt.Println(IsValidTopicName("#"))

	test.Equal(t, false, IsValidTopicName("#"))
	test.Equal(t, false, IsValidTopicName("##"))
	test.Equal(t, false, IsValidTopicName("@"))
	test.Equal(t, false, IsValidTopicName("!"))
	test.Equal(t, false, IsValidTopicName("&"))
	test.Equal(t, false, IsValidTopicName("1A#ephem"))
}
