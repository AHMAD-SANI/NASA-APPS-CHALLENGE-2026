"""Database-aware bounds for batched FireScope persistence."""

import sqlite3

from django.db import connection


DEFAULT_BATCH_CAP = 500


def max_sql_parameters() -> int:
    """Return the active backend's bind-parameter limit when it is known."""
    limit = connection.features.max_query_params
    if connection.vendor == 'sqlite':
        if connection.connection is None:
            connection.ensure_connection()
        dbapi_connection = connection.connection
        if hasattr(dbapi_connection, 'getlimit'):
            runtime_limit = dbapi_connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER)
            limit = min(limit, runtime_limit) if limit else runtime_limit
    return limit or DEFAULT_BATCH_CAP


def safe_sql_batch_size(model, objects, batch_cap: int = DEFAULT_BATCH_CAP) -> int:
    """Bound an object's IN-clause keys and multi-row INSERT parameters."""
    if not objects:
        return 1
    fields = [
        field for field in model._meta.concrete_fields
        if not field.generated and not (field.auto_created and field.primary_key)
    ]
    params_per_row = max(1, len(fields))
    parameter_limit = max_sql_parameters()
    size = min(
        batch_cap,
        parameter_limit,
        max(1, parameter_limit // params_per_row),
    )
    backend_size = connection.ops.bulk_batch_size(fields, objects)
    if backend_size:
        size = min(size, backend_size)
    return max(1, size)


def safe_in_batch_size(parameters_per_item: int = 1, batch_cap: int = DEFAULT_BATCH_CAP) -> int:
    """Bound key lists used by IN filters independently from INSERT rows."""
    parameter_limit = max_sql_parameters()
    return max(1, min(batch_cap, parameter_limit // max(1, parameters_per_item)))
