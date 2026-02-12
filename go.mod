module github.com/dgraph-io/badger/v4

go 1.24.0

toolchain go1.25.0

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgraph-io/ristretto/v2 v2.4.0
	github.com/dustin/go-humanize v1.0.1
	github.com/google/flatbuffers v25.12.19+incompatible
	github.com/klauspost/compress v1.18.4
	github.com/spf13/cobra v1.10.2
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/contrib/zpages v0.65.0
	go.opentelemetry.io/otel v1.40.0
	golang.org/x/net v0.50.0
	golang.org/x/sys v0.41.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/sdk v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v4.0.0 // see #1888 and #1889

retract v4.3.0 // see #2113 and #2121
