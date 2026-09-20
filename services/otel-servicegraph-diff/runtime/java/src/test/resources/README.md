# Behavioral fixtures

`lifecycle-golden.json`, `ingest-golden.json`, and `canonical-golden.json` are
frozen behavioral expectations captured from the Python implementation during
the native Java migration. The Python source baseline is Git revision
`15124a6`; the completed validation and comparison are recorded under
`benchmarks/results/java-migration-2026-09-17`.

The Python lifecycle implementation and fixture generators were removed after
parity was established. Keep these fixtures independent of the Java code under
test. Add explicit expectations for intentional behavior changes and review
event-contract compatibility rather than regenerating expected values from
the implementation being tested.

`src/main/resources/semantic-registry.json` remains generated from the shared
semantic registry by `python -m tools.semconv_codegen`.
