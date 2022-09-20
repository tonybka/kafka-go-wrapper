package kafka_go_wrapper

import "encoding/json"

type ResponseMessage struct {
	Error string `json:"error,omitempty"`
	Data  []byte `json:"data,omitempty"`
}

func (res ResponseMessage) Bytes() []byte {
	responseInBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}
	return responseInBytes
}
