# externalscaler.proto

`externalscaler.proto` is KEDA's external scaler protocol, which the broker
serves so that KEDA can scale workers on the backlog of a queue. The file is
vendored: it is the one KEDA publishes, and it is not written or edited here.

| | |
| --- | --- |
| Source | https://github.com/kedacore/keda/blob/v2.21.0/pkg/scalers/externalscaler/externalscaler.proto |
| Tag | `v2.21.0` (commit `626ded5d783108bedf647e20bbc83f8e458e02e4`) |
| SHA-256 | `0e1c84a7ee1d4745eefe1fd7d16e2cbbd02763dc0cfc4c4c37d92d026deb9253` |
| License | Apache-2.0, as KEDA |

Check the file against the checksum with:

```sh
shasum -a 256 proto/externalscaler/externalscaler.proto
```

## The local change

None to the file. Upstream declares `option go_package = ".;externalscaler";`,
which would put the generated code in the current directory. Buf's managed mode
replaces the option while it generates (the `go_package_prefix` override in
`buf.gen.yaml`), so the Go package is
`github.com/jason-te-sde/distributed-job-queue/internal/gen/externalscaler`, next to
the code generated from this repository's own protos. The replacement is
visible in the descriptor embedded in the generated file, and nowhere in this
one.

`buf.yaml` leaves the file out of `buf lint` and `buf breaking`: its names do
not follow the style rules (`ScaledObjectRef` is the request of four RPCs, and
fields are in camelCase), and the wire contract belongs to KEDA.

## Updating

1. Fetch the file at the new KEDA tag, for example with
   `gh api "repos/kedacore/keda/contents/pkg/scalers/externalscaler/externalscaler.proto?ref=<tag>" --jq .content | base64 -d`.
2. Replace `externalscaler.proto` with it, unchanged.
3. Record the tag, its commit and the new checksum in the table above.
4. Run `make gen`, and commit the generated code with the proto.
