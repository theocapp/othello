"""Structured logging configuration for the signal intelligence system."""

import logging
import logging.config
import sys
from datetime import datetime
from typing import Any
import json


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "lineno", "funcName", "created",
                "msecs", "relativeCreated", "thread", "threadName",
                "processName", "process", "exc_info", "exc_text", "stack_info",
                "message", "asctime",
            }:
                log_data[key] = value

        return json.dumps(log_data)


class CorrelationIdFilter(logging.Filter):
    """Filter to add correlation ID to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation ID to record if present in context."""
        # In a real implementation, you'd get this from a context variable
        # For now, we'll add a placeholder
        if not hasattr(record, "correlation_id"):
            record.correlation_id = getattr(record, "correlation_id", None)
        return True


def setup_logging(
    level: str = "INFO",
    json_format: bool = True,
    log_file: str | None = None,
) -> None:
    """Setup structured logging configuration.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Whether to use JSON format for logs
        log_file: Optional file path to write logs to
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    formatter = JSONFormatter() if json_format else logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # File handler (if specified)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Configure root logger
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "logging_utils.JSONFormatter" if json_format else "logging.Formatter",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "default",
                "stream": "ext://sys.stdout",
            },
        },
        "root": {
            "level": log_level,
            "handlers": ["console"],
        },
        "loggers": {
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "uvicorn.access": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }

    if log_file:
        logging_config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "level": log_level,
            "formatter": "default",
            "filename": log_file,
        }
        logging_config["root"]["handlers"].append("file")

    logging.config.dictConfig(logging_config)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_with_context(
    logger: logging.Logger,
    level: str,
    message: str,
    **context: Any,
) -> None:
    """Log a message with additional context.

    Args:
        logger: Logger instance
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        message: Log message
        **context: Additional context to include in the log
    """
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(message, **context)


class LoggerMixin:
    """Mixin class to add logging capabilities to any class."""

    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class."""
        return get_logger(self.__class__.__name__)


# ============================================================================
# Error handling utilities
# ============================================================================

class AppError(Exception):
    """Base exception for application errors."""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ):
        """Initialize application error.

        Args:
            message: Error message
            error_code: Optional error code
            details: Optional error details
            correlation_id: Optional correlation ID for tracking
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.correlation_id = correlation_id

    def to_dict(self) -> dict[str, Any]:
        """Convert error to dictionary.

        Returns:
            Dictionary representation of the error
        """
        return {
            "error": self.message,
            "error_code": self.error_code,
            "details": self.details,
            "correlation_id": self.correlation_id,
        }


class ValidationError(AppError):
    """Exception for validation errors."""

    def __init__(
        self,
        message: str,
        field: str | None = None,
        value: Any = None,
        **kwargs,
    ):
        """Initialize validation error.

        Args:
            message: Error message
            field: Field that failed validation
            value: Value that failed validation
            **kwargs: Additional error details
        """
        details = kwargs.get("details", {})
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)
        kwargs["details"] = details
        super().__init__(message, error_code="VALIDATION_ERROR", **kwargs)


class DatabaseError(AppError):
    """Exception for database errors."""

    def __init__(
        self,
        message: str,
        query: str | None = None,
        **kwargs,
    ):
        """Initialize database error.

        Args:
            message: Error message
            query: Query that caused the error
            **kwargs: Additional error details
        """
        details = kwargs.get("details", {})
        if query:
            details["query"] = query
        kwargs["details"] = details
        super().__init__(message, error_code="DATABASE_ERROR", **kwargs)


class ExternalServiceError(AppError):
    """Exception for external service errors."""

    def __init__(
        self,
        message: str,
        service: str | None = None,
        status_code: int | None = None,
        **kwargs,
    ):
        """Initialize external service error.

        Args:
            message: Error message
            service: Service that caused the error
            status_code: HTTP status code
            **kwargs: Additional error details
        """
        details = kwargs.get("details", {})
        if service:
            details["service"] = service
        if status_code:
            details["status_code"] = status_code
        kwargs["details"] = details
        super().__init__(message, error_code="EXTERNAL_SERVICE_ERROR", **kwargs)


def handle_error(
    error: Exception,
    logger: logging.Logger | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Handle an error and return a standardized error response.

    Args:
        error: Exception to handle
        logger: Optional logger to log the error
        correlation_id: Optional correlation ID for tracking

    Returns:
        Dictionary with error information
    """
    if logger:
        logger.error(
            f"Error occurred: {str(error)}",
            error_type=type(error).__name__,
            correlation_id=correlation_id,
        )

    if isinstance(error, AppError):
        return error.to_dict()

    return {
        "error": str(error),
        "error_code": "INTERNAL_ERROR",
        "details": {},
        "correlation_id": correlation_id,
    }
