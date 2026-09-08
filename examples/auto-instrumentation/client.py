import httpx


def main() -> None:
    with httpx.Client(base_url="http://127.0.0.1:18081", timeout=10.0) as client:
        for _ in range(3):
            response = client.get("/users/123")
            response.raise_for_status()
            print(response.json())


if __name__ == "__main__":
    main()
