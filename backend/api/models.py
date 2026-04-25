"""Pydantic models for API request/response validation."""

from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict


# ============================================================================
# Query & Timeline Models
# ============================================================================

class QueryRequest(BaseModel):
    """Request model for query endpoint."""
    question: str = Field(..., min_length=1, max_length=2000, description="The question to answer")
    topic: Optional[str] = Field(None, description="Topic filter (geopolitics, economics)")
    region_context: Optional[str] = Field(None, max_length=200, description="Geographic context")
    hotspot_id: Optional[str] = Field(None, max_length=100, description="Hotspot ID for context")
    story_event_id: Optional[str] = Field(None, max_length=100, description="Story event ID for context")
    source_urls: Optional[list[str]] = Field(None, max_length=12, description="Source URLs to ground the query")
    attention_window: Optional[str] = Field(None, pattern=r"^(24h|7d|30d|90d|365d)$", description="Time window for attention")

    @field_validator('source_urls')
    @classmethod
    def validate_source_urls(cls, v):
        if v is None:
            return v
        cleaned = []
        for url in v:
            if isinstance(url, str) and url.strip().startswith('http'):
                cleaned.append(url.strip())
        return cleaned[:12]

    @field_validator('topic')
    @classmethod
    def validate_topic(cls, v):
        if v is None:
            return v
        valid_topics = {'geopolitics', 'economics', 'conflict'}
        topic_lower = v.lower().strip()
        if topic_lower == 'conflict':
            return 'geopolitics'
        if topic_lower in valid_topics:
            return topic_lower
        return v


class TimelineRequest(BaseModel):
    """Request model for timeline endpoint."""
    query: str = Field(..., min_length=1, max_length=500, description="Timeline query")
    topic: Optional[str] = Field(None, description="Topic filter")
    limit: int = Field(18, ge=1, le=100, description="Maximum number of events")


# ============================================================================
# Event Models
# ============================================================================

class MergeEventRequest(BaseModel):
    """Request model for merging events."""
    merge_with_event_id: str = Field(..., min_length=1, max_length=100, description="Event ID to merge with")


class SplitArticleRequest(BaseModel):
    """Request model for splitting an article from an event."""
    article_url: str = Field(..., min_length=1, max_length=1000, description="Article URL to split")


class GetEventsRequest(BaseModel):
    """Request model for getting events."""
    limit: int = Field(12, ge=1, le=100, description="Maximum number of events")
    include_factual: bool = Field(False, description="Include factual claim contradictions")


class GetStructuredEventsRequest(BaseModel):
    """Request model for getting structured events."""
    days: int = Field(3, ge=1, le=365, description="Number of days to look back")
    limit: int = Field(12, ge=1, le=500, description="Maximum number of events")
    country: Optional[str] = Field(None, max_length=100, description="Country filter")
    event_type: Optional[str] = Field(None, max_length=100, description="Event type filter")


class GetMaterializedStoryClustersRequest(BaseModel):
    """Request model for getting materialized story clusters."""
    topic: Optional[str] = Field(None, description="Topic filter")
    window_hours: Optional[int] = Field(None, ge=1, le=8760, description="Time window in hours")
    limit: int = Field(40, ge=1, le=200, description="Maximum number of clusters")


class GetCanonicalEventsRequest(BaseModel):
    """Request model for getting canonical events."""
    topic: Optional[str] = Field(None, description="Topic filter")
    status: Optional[str] = Field(None, description="Status filter")
    limit: int = Field(40, ge=1, le=200, description="Maximum number of events")


class GetCanonicalEventsMapRequest(BaseModel):
    """Request model for getting canonical events map."""
    days: int = Field(7, ge=1, le=365, description="Number of days to look back")
    limit: int = Field(500, ge=1, le=1000, description="Maximum number of events")


class GetTopicEventsRequest(BaseModel):
    """Request model for getting events for a topic."""
    topic: str = Field(..., min_length=1, max_length=50, description="Topic name")
    limit: int = Field(8, ge=1, le=50, description="Maximum number of events")


# ============================================================================
# Map & Coverage Models
# ============================================================================

class GetRegionAttentionRequest(BaseModel):
    """Request model for getting region attention."""
    window: str = Field("24h", pattern=r"^(24h|7d|30d|90d|365d)$", description="Time window")


class GetHotspotAttentionMapRequest(BaseModel):
    """Request model for getting hotspot attention map."""
    window: str = Field("24h", pattern=r"^(24h|7d|30d|90d|365d)$", description="Time window")
    start: Optional[str] = Field(None, description="Start date (ISO format)")
    end: Optional[str] = Field(None, description="End date (ISO format)")
    days: Optional[int] = Field(None, ge=1, le=365, description="Number of days")

    @field_validator('start', 'end')
    @classmethod
    def validate_date(cls, v):
        if v is None:
            return v
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('Invalid date format. Use ISO format (e.g., 2024-01-01T00:00:00Z)')
        return v


# ============================================================================
# Instability & Correlation Models
# ============================================================================

class GetInstabilityRequest(BaseModel):
    """Request model for getting instability data."""
    days: int = Field(3, ge=1, le=365, description="Number of days to look back")


class GetInstabilityDetailRequest(BaseModel):
    """Request model for getting instability detail for a country."""
    country: str = Field(..., min_length=1, max_length=100, description="Country name")
    days: int = Field(3, ge=1, le=365, description="Number of days to look back")


class GetCorrelationsRequest(BaseModel):
    """Request model for getting correlations."""
    days: int = Field(3, ge=1, le=365, description="Number of days to look back")


# ============================================================================
# Entity Models
# ============================================================================

class GetEntityMentionsRequest(BaseModel):
    """Request model for getting entity mentions."""
    entity: str = Field(..., min_length=1, max_length=200, description="Entity name")
    topic: Optional[str] = Field(None, description="Topic filter")
    limit: int = Field(20, ge=1, le=100, description="Maximum number of mentions")


class GetEntityCooccurrencesRequest(BaseModel):
    """Request model for getting entity co-occurrences."""
    entity: str = Field(..., min_length=1, max_length=200, description="Entity name")
    topic: Optional[str] = Field(None, description="Topic filter")
    limit: int = Field(20, ge=1, le=100, description="Maximum number of co-occurrences")


# ============================================================================
# Briefing Models
# ============================================================================

class GetBriefingRequest(BaseModel):
    """Request model for getting a briefing."""
    topic: str = Field(..., min_length=1, max_length=50, description="Topic name")
    window: str = Field("24h", pattern=r"^(24h|7d|30d|90d|365d)$", description="Time window")


# ============================================================================
# Headline Models
# ============================================================================

class GetHeadlinesRequest(BaseModel):
    """Request model for getting headlines."""
    topic: Optional[str] = Field(None, description="Topic filter")
    limit: int = Field(20, ge=1, le=100, description="Maximum number of headlines")


# ============================================================================
# Analytics Models
# ============================================================================

class GetAnalyticsRequest(BaseModel):
    """Request model for getting analytics."""
    topic: Optional[str] = Field(None, description="Topic filter")
    window: str = Field("24h", pattern=r"^(24h|7d|30d|90d|365d)$", description="Time window")


# ============================================================================
# Response Models
# ============================================================================

class ErrorResponse(BaseModel):
    """Standard error response model."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    status_code: int = Field(..., description="HTTP status code")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "error": "Invalid request",
            "detail": "The provided parameters are invalid",
            "status_code": 400
        }
    })


class SuccessResponse(BaseModel):
    """Standard success response model."""
    status: str = Field("success", description="Response status")
    message: Optional[str] = Field(None, description="Success message")
    data: Optional[dict[str, Any]] = Field(None, description="Response data")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "status": "success",
            "message": "Operation completed successfully",
            "data": {}
        }
    })
