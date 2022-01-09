module github.com/dgraph-io/badger/v3

go 1.12

// replace github.com/dgraph-io/ristretto => /home/amanbansal/go/src/github.com/dgraph-io/ristretto

require (
	github.com/cespare/xxhash v1.1.0
	github.com/dgraph-io/ristretto v0.1.0
	github.com/dustin/go-humanize v1.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/flatbuffers v2.0.5+incompatible
	github.com/klauspost/compress v1.13.6
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.4 // indirect
	github.com/spf13/cobra v1.3.0
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.23.0
	golang.org/x/net v0.0.0-20220107192237-5cfca573fb4d
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
	google.golang.org/genproto v0.0.0-20220107163113-42d7afdf6368 // indirect
	google.golang.org/grpc v1.43.0 // indirect
)
