from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, Filter, FieldCondition, MatchValue  # <- PointStruct import
from qdrant_client.models import VectorParams, Distance
from qdrant_client.http.models import PayloadSchemaType
import os
load_dotenv()


QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")


class QdrantUtils:
    def __init__(self):
        self.COLLECTION_NAME = "job_listings"
        self.qdrant = self.connect_qdrant()
        # self.dummy_vector = np.zeros(1536).tolist()
    
    def connect_qdrant(self):
        qdrant = QdrantClient(
            url="https://6b5de3cc-b9cc-4118-8fdb-ba1975af11ca.us-east-1-1.aws.cloud.qdrant.io",
            api_key=QDRANT_API_KEY,
            timeout=60.0
        )
        return qdrant
    
    def recreate_collection(self):
        self.qdrant.recreate_collection(
            collection_name=self.COLLECTION_NAME,
            vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
        )
        self.qdrant.create_payload_index(
                collection_name=self.COLLECTION_NAME,
                field_name="embedding",
                field_schema=PayloadSchemaType.BOOL
            )
        self.qdrant.create_payload_index(
                collection_name=self.COLLECTION_NAME,
                field_name="enriched",
                field_schema=PayloadSchemaType.BOOL
            )

# if __name__ == '__main__':
#     scrape_job = ScrapeJob()
#     scrape_job.search_jobs(int(sys.argv[1]), sys.argv[2])
