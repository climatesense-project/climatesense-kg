"""Shared formatting helpers for long-running pipeline stages."""


def format_duration(seconds: float | None) -> str:
    """Format a duration for compact progress logs."""

    if seconds is None:
        return "n/a"
    rounded = max(0, round(seconds))
    minutes, remaining_seconds = divmod(rounded, 60)
    hours, remaining_minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {remaining_minutes}m"
    if minutes:
        return f"{minutes}m {remaining_seconds}s"
    return f"{remaining_seconds}s"
