"""Placeholder referenced by the CustomDatabase service's `sourcePythonClass`.

The OM server only stores the string; nothing imports it unless someone schedules an ingestion
pipeline against this service from the OM UI, which this project does not use.
"""


class NoopSource:  # pragma: no cover
    pass
