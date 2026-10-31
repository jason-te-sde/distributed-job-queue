module github.com/jason-te-sde/distributed-job-queue/bench

go 1.26.0

replace github.com/jason-te-sde/distributed-job-queue => ../

require (
	github.com/HdrHistogram/hdrhistogram-go v1.4.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/jason-te-sde/distributed-job-queue v0.0.0-00010101000000-000000000000
	github.com/riverqueue/river v0.50.0
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.50.0
	google.golang.org/grpc v1.85.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.25.0 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/riverqueue/river/riverdriver v0.50.0 // indirect
	github.com/riverqueue/river/rivershared v0.50.0 // indirect
	github.com/riverqueue/river/rivertype v0.50.0 // indirect
	github.com/tidwall/gjson v1.20.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
