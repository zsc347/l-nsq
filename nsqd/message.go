package nsqd

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2
)

type MessageID [MsgIDLength]byte
