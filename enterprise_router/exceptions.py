class RouterError(Exception):
    """Base exception for the package."""


class ValidationError(RouterError):
    """Raised when inputs fail schema or field validation."""


class RegistrationError(RouterError):
    """Raised for self-registration failures."""


class AccessError(RouterError):
    """Raised when an agent cannot perform an action."""
