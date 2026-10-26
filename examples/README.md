# StreamBus examples

Two minimal applications showing the documented producer/consumer flow:

1. `StreamBus.Examples.Producer` — owns the topology. At startup it
   declares the plain stream `orders` (7-day retention) and the
   3-partition super stream `payments-super`, then seeds a few messages
   into both and exits.
2. `StreamBus.Examples.Consumer` — a different application. It declares
   nothing: the `orders` endpoint adopts the broker's stream as-is, and
   the `payments-super` endpoint reads all three partitions with the
   broker-reported count. It runs until Ctrl+C.

Each application is self-contained (message records are duplicated); in a
real solution they would share a contracts project.

## Run

Prerequisite: RabbitMQ 4.x with streams enabled reachable on
`localhost:5552`, and a dedicated non-`guest` user. The committed values
(`app` / `password`) match the repo's local dev broker; adjust
`UserName`/`Password` in each `appsettings.json`, or override them with
the environment variables `StreamBus__UserName` / `StreamBus__Password`.
Each example reads these values in its own `Program.cs` and passes them to
`cfg.Host`; StreamBus itself never binds `appsettings.json`.

Run the producer first (it owns the topology), then the consumer:

```pwsh
dotnet run --project examples/StreamBus.Examples.Producer
dotnet run --project examples/StreamBus.Examples.Consumer
```

Expected: the producer logs 5 publishes to `orders` and 5 to
`payments-super`; the consumer then logs each message with the partition
and offset it was read from. The payments keyed by the same account id
always show the same partition.

Notes:

- Consumer groups are declared verbatim. Running two consumer instances
  with the same groups scales out (partitions are split); changing the
  groups gives a second application an independent cursor that receives
  every message again.
- The consumer resumes from the broker-stored offset, so restarting it
  does not replay what it already processed (until a new group is used).
