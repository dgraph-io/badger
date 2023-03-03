module github.com/dgraph-io/badger/v4

go 1.19

require (
	github.com/cespare/xxhash v1.1.0
	github.com/dgraph-io/ristretto v0.1.1
	github.com/dustin/go-humanize v1.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.3.1
	github.com/golang/snappy v0.0.3
	github.com/google/flatbuffers v1.12.1
	github.com/klauspost/compress v1.12.3
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.5
	golang.org/x/net v0.7.0
	golang.org/x/sys v0.5.0
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/google/go-cmp v0.5.4 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	google.golang.org/genproto v0.0.0-20190425155659-357c62f0e4bb // indirect
	google.golang.org/grpc v1.20.1 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
)

retract v4.0.0 // see #1888 and #1889
