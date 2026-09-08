from fastapi import FastAPI

app = FastAPI()


@app.get("/users/{user_id}")
def get_user(user_id: str) -> dict[str, str]:
    return {"user_id": user_id}
