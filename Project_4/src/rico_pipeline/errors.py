class AuditError(RuntimeError):
    """Raised when the post-load audit must halt the DAG."""
