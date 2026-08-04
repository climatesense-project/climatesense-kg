"""Canonical RDF serialization format metadata."""

RDF_FORMAT_EXTENSIONS: dict[str, frozenset[str]] = {
    "turtle": frozenset({".ttl"}),
    "nt": frozenset({".nt"}),
    "rdf/xml": frozenset({".rdf", ".xml"}),
    "n3": frozenset({".n3"}),
}

RDF_EXTENSION_CONTENT_TYPES: dict[str, str] = {
    ".ttl": "text/turtle",
    ".nt": "application/n-triples",
    ".rdf": "application/rdf+xml",
    ".xml": "application/rdf+xml",
    ".n3": "text/n3",
}
