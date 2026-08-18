"""Single registration catalog for provider configuration and implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config.schemas import (
    FileProviderConfig,
    GitHubProviderConfig,
    GraphQLProviderConfig,
    HttpProviderConfig,
    XWikiProviderConfig,
)
from .providers.base import BaseProvider
from .providers.file import FileProvider
from .providers.github import GitHubProvider
from .providers.graphql import GraphQLProvider
from .providers.http import HttpProvider
from .providers.xwiki import XWikiProvider


@dataclass(frozen=True)
class ProviderRegistration:
    """Configuration and runtime implementation for one discriminator."""

    config_type: type[Any]
    provider_type: type[BaseProvider[Any]]


PROVIDER_REGISTRATIONS = {
    "file": ProviderRegistration(FileProviderConfig, FileProvider),
    "github": ProviderRegistration(GitHubProviderConfig, GitHubProvider),
    "graphql": ProviderRegistration(GraphQLProviderConfig, GraphQLProvider),
    "http": ProviderRegistration(HttpProviderConfig, HttpProvider),
    "xwiki": ProviderRegistration(XWikiProviderConfig, XWikiProvider),
}
