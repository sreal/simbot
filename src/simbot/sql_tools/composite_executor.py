"""Composite query execution: fan out to N referenced sub-queries, sequentially."""

import logging
import re
from typing import Any, Dict

from .executor import QueryExecutor
from .loader import QueryLoader
from .models import (
    CompositeQueryDefinition,
    CompositeQueryResult,
    ExecutionContext,
    QueryResult,
)


logger = logging.getLogger(__name__)


_WHOLE_TOKEN_PATTERN = re.compile(r"^\{([A-Za-z_][A-Za-z0-9_]*)\}$")


class CompositeQueryExecutor:
    """Run a composite definition by executing each referenced sub-query in turn.

    Errors are best-effort: a failing sub-query produces a failed QueryResult
    for that label, while successful siblings still return their data. The
    composite-level success flag is True if any sub-query succeeded.
    """

    def __init__(self, query_executor: QueryExecutor, loader: QueryLoader):
        self.query_executor = query_executor
        self.loader = loader

    def execute(
        self,
        composite: CompositeQueryDefinition,
        composite_params: Dict[str, Any],
        context: ExecutionContext,
    ) -> CompositeQueryResult:
        sub_results: Dict[str, QueryResult] = {}

        for sub in composite.queries:
            sub_def = self.loader.get_query_by_id(sub.ref)
            if sub_def is None:
                # Loader pre-validation should make this unreachable, but guard anyway.
                logger.error(
                    "Composite %s/%s: referenced query '%s' not found at execute time",
                    composite.name, sub.label, sub.ref,
                )
                sub_results[sub.label] = QueryResult(
                    success=False,
                    error=f"Referenced query '{sub.ref}' not found",
                    error_code="COMPOSITE_REF_MISSING",
                    correlation_id=context.correlation_id,
                )
                continue

            sub_params = self._render_params(sub.params, composite_params)

            try:
                sub_result = self.query_executor.execute(sub_def, sub_params, context)
            except Exception as e:
                logger.exception(
                    "Composite %s/%s: sub-query execution raised",
                    composite.name, sub.label,
                )
                sub_result = QueryResult(
                    success=False,
                    error=f"{type(e).__name__}: {e}",
                    error_code="COMPOSITE_SUB_EXCEPTION",
                    correlation_id=context.correlation_id,
                )

            sub_results[sub.label] = sub_result

        any_success = any(r.success for r in sub_results.values())

        return CompositeQueryResult(
            success=any_success,
            composite_name=composite.name,
            sub_results=sub_results,
            correlation_id=context.correlation_id,
        )

    @staticmethod
    def _render_params(
        template: Dict[str, str],
        values: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Substitute {Name} tokens against composite-level params.

        If a value is exactly '{Name}' and Name is a known composite param,
        the original (typed) value is passed through. Otherwise the value is
        treated as a string template and rendered via str.format(**values).
        """
        out: Dict[str, Any] = {}
        for k, v in template.items():
            if isinstance(v, str):
                whole = _WHOLE_TOKEN_PATTERN.match(v)
                if whole and whole.group(1) in values:
                    out[k] = values[whole.group(1)]
                else:
                    try:
                        out[k] = v.format(**values)
                    except KeyError as e:
                        raise KeyError(
                            f"Composite param substitution failed for '{k}'='{v}': "
                            f"missing composite parameter {e}"
                        )
            else:
                out[k] = v
        return out
