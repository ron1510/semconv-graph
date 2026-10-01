"""Errors raised while reconstructing generated semantic models."""


class SemanticModelError(ValueError):
    """Base error for semantic model reconstruction failures."""


class UnknownSemanticTypeError(SemanticModelError):
    """Raised when no generated model owns a semantic type or relationship."""


class SemanticModelValidationError(SemanticModelError):
    """Raised when stored semantic data cannot construct its generated model."""
