from app_factory import create_app
from chroma import get_collection_stats
from core.config import TOPICS
from core.map_state import MAP_ATTENTION_CACHE as _MAP_ATTENTION_CACHE
from core.map_state import STORY_LOCATION_INDEX_CACHE as _STORY_LOCATION_INDEX_CACHE
from core.scheduler import build_scheduler, build_worker_scheduler

app = create_app()
