# Distributed contribution coalescing measurement

`java-e2e-benchmark.json` records the controlled workload run by the complete
disposable E2E suite on 2026-10-06. The test used one Flink subtask, a five-second
coalescing interval, 16 entities, four contributors, one warmup batch, and three
measured repeated batches.

The coalescer received 768 contributions during the measured window, suppressed
576 unchanged observations, forwarded 192 new observations, and later forwarded
192 latest observations from timers. The lifecycle vertex received 144 records
during the same sampled window. No measured public graph event was expected or
emitted after warmup, and the final graph hash matched the uncoalesced semantic
expectation.

The latest sampled checkpoint completed in 72 ms with 108,075 checkpointed
bytes and 437,361 bytes of full state. These values describe this short local
Kind run. They establish behavioral reduction and checkpoint/recovery coverage,
not production capacity or long-duration stability.
