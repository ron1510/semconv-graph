import httpx
from etl_context import configure_etl_instrumentation, etl_part, etl_run


def main() -> None:
    configure_etl_instrumentation()
    with (
        etl_run(run_id="demo-run-001", name="Scheduled customer extraction"),
        etl_part(part_run_id="load-customers", name="Load customers"),
        httpx.Client(base_url="http://127.0.0.1:18081", timeout=10.0) as client,
    ):
        for _ in range(3):
            response = client.get("/users/123")
            response.raise_for_status()
            print(response.json())


if __name__ == "__main__":
    main()

