"""Data providers for fetching raw data from different sources."""

from .file import FileProvider
from .github import GitHubProvider
from .graphql import GraphQLProvider
from .xwiki import XWikiProvider

__all__ = [
    "FileProvider",
    "GitHubProvider",
    "GraphQLProvider",
    "XWikiProvider",
]
