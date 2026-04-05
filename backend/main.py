from app_factory import create_app
from chroma import get_collection_stats
from core.config import TOPICS
from core.scheduler import build_scheduler, build_worker_scheduler
from services.map_service import _MAP_ATTENTION_CACHE, _STORY_LOCATION_INDEX_CACHE

app = create_app()
