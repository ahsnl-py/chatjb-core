import uvicorn
import os
from fastapi import FastAPI
from mangum import Mangum
from pydantic import BaseModel
from service.rag_service import QueryResponse, RAG
from service.db_service import DbService
from service.api_service import ApiService
from dotenv import load_dotenv

app = FastAPI()

params = {
    "conn_str": os.getenv("MONGODB_URI"),
    "dbname": os.getenv("MONGODB_DB_NAME")
}
db = DbService(**params)
api_service = ApiService("")
rag = RAG(db, api_service)
handler = Mangum(app)  # Entry point for AWS Lambda.

class SubmitQueryRequest(BaseModel):
    query_text: str


@app.get("/")
def index():
    return {"Hello": "World"}

@app.post("/submit_query")
def submit_query_endpoint(request: SubmitQueryRequest) -> QueryResponse:
    query_response = rag.vector_query(request.query_text, "jobcz_details_embedded")
    return query_response


if __name__ == "__main__":
    # Run this as a server directly.
    port = 8000
    print(f"Running the FastAPI server on port {port}.")
    uvicorn.run("app_api_handler:app", host="0.0.0.0", port=port)