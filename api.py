from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch 

app = FastAPI()
es = Elasticsearch("http://localhost:9200")

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/api/stocks")
async def get_stocks(num_rows: int = Query(default=10, ge=1)):
    try:
        response = es.search(
            index = "stock-data",
            body = {
                "query": {
                    "match_all": {}
                },
                "sort": [{"timestamp": {"order": "desc"}}],
                "size": num_rows
            }
        )
        hits = response["hits"]["hits"]
        stocks = [hit["_source"] for hit in hits]
        return stocks
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving stocks: {str(e)}")

# For running directly as a script
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)