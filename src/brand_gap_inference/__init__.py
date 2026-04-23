"""Foundation package for Brand Gap Inference."""

from .amazon import AmazonProductConnector, canonicalize_amazon_product_url, extract_amazon_asin
from .amazon_normalizer import AmazonListingNormalizer
from .contracts import SCHEMA_FILES, ValidationIssue, assert_valid, validate_document
from .ingestion import IngestionService
from .normalization import BatchNormalizer, write_normalization_artifacts
from .raw_store import FilesystemRawStore
from .run_metadata import RunManifest, RunTaskEnvelope
from .taxonomy import TaxonomyAssigner, write_taxonomy_artifacts

__all__ = [
    "AmazonProductConnector",
    "AmazonListingNormalizer",
    "BatchNormalizer",
    "FilesystemRawStore",
    "IngestionService",
    "SCHEMA_FILES",
    "TaxonomyAssigner",
    "ValidationIssue",
    "RunManifest",
    "RunTaskEnvelope",
    "assert_valid",
    "canonicalize_amazon_product_url",
    "extract_amazon_asin",
    "validate_document",
    "write_normalization_artifacts",
    "write_taxonomy_artifacts",
]
