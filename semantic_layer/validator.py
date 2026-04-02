"""Validates query results for completeness and sanity."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from utils.guardrails import mask_pii_dataframe
from utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    row_count: int
    warnings: list[str]
    data: pd.DataFrame | None


class ResultValidator:
    """Validates and sanitizes query result DataFrames."""

    def validate(self, df: pd.DataFrame, concept_name: str) -> ValidationResult:
        warnings: list[str] = []

        if df.empty:
            return ValidationResult(
                is_valid=True,
                row_count=0,
                warnings=["No results returned — concept may not apply to this entity"],
                data=df,
            )

        row_count = len(df)

        # Check for excessive nulls
        for col in df.columns:
            null_pct = df[col].isnull().sum() / row_count * 100
            if null_pct > 50:
                warnings.append(
                    f"Column '{col}' is {null_pct:.0f}% null — data may be incomplete"
                )

        # Mask PII
        df = mask_pii_dataframe(df)

        logger.info(
            "result_validated",
            concept=concept_name,
            rows=row_count,
            warnings=len(warnings),
        )

        return ValidationResult(
            is_valid=True,
            row_count=row_count,
            warnings=warnings,
            data=df,
        )


result_validator = ResultValidator()
