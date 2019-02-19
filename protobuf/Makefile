all: gen

.PHONY : gen

gen:
	protoc raft.proto --go_out=plugins=grpc:.
