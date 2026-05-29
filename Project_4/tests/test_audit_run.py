import pytest
from rico_pipeline.audit import AuditFailed, persist_audit_sql

def test_persist_sql_shape():
    sql, params = persist_audit_sql("u1", "duplicate_detection", True, {"x": 1})
    assert "INSERT INTO audit_results" in sql
    assert params[0] == "u1" and params[2] is True

def test_auditfailed_is_exception():
    assert issubclass(AuditFailed, Exception)
