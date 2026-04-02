"""Semantic abstraction layer: business concept definitions.

Each concept maps a business-friendly name to the SQL logic that
computes it.  Agents compose investigations using these concepts
rather than writing raw SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SemanticConcept:
    """A single business concept backed by SQL logic."""

    name: str
    description: str
    sql_template: str
    parameters: list[str] = field(default_factory=list)
    domain: str = ""  # claims | provider | member | eligibility | cross


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CLAIMS DOMAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HIGH_COST_CLAIMS = SemanticConcept(
    name="high_cost_claims",
    description="Claims whose allowed amount exceeds the 95th percentile for their procedure code and region",
    domain="claims",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    WITH pctl AS (
        SELECT procedure_code, service_state,
               PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY allowed_amount) AS p95
        FROM claims
        WHERE service_date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY procedure_code, service_state
    )
    SELECT c.claim_id, c.provider_npi, c.member_id, c.procedure_code,
           c.allowed_amount, p.p95,
           ROUND(c.allowed_amount / NULLIF(p.p95, 0), 2) AS cost_ratio
    FROM claims c
    JOIN pctl p ON c.procedure_code = p.procedure_code
                AND c.service_state = p.service_state
    WHERE c.provider_npi = %(provider_npi)s
      AND c.service_date BETWEEN %(start_date)s AND %(end_date)s
      AND c.allowed_amount > p.p95
    ORDER BY cost_ratio DESC
    """,
)

UPCODING_PATTERN = SemanticConcept(
    name="upcoding_pattern",
    description="Provider E&M code distribution vs specialty peer average, flagging systematic use of higher-level codes",
    domain="claims",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    WITH provider_dist AS (
        SELECT procedure_code,
               COUNT(*) AS provider_count,
               COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS provider_pct
        FROM claims
        WHERE provider_npi = %(provider_npi)s
          AND service_date BETWEEN %(start_date)s AND %(end_date)s
          AND procedure_code IN ('99211','99212','99213','99214','99215')
        GROUP BY procedure_code
    ),
    peer_dist AS (
        SELECT c.procedure_code,
               COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS peer_pct
        FROM claims c
        JOIN providers p ON c.provider_npi = p.npi
        WHERE p.specialty = (SELECT specialty FROM providers WHERE npi = %(provider_npi)s)
          AND c.service_date BETWEEN %(start_date)s AND %(end_date)s
          AND c.procedure_code IN ('99211','99212','99213','99214','99215')
        GROUP BY c.procedure_code
    )
    SELECT pd.procedure_code,
           ROUND(pd.provider_pct, 1) AS provider_pct,
           ROUND(pr.peer_pct, 1)     AS peer_pct,
           ROUND(pd.provider_pct - pr.peer_pct, 1) AS delta
    FROM provider_dist pd
    JOIN peer_dist pr ON pd.procedure_code = pr.procedure_code
    ORDER BY pd.procedure_code
    """,
)

DUPLICATE_CLAIMS = SemanticConcept(
    name="duplicate_claims",
    description="Claims with identical provider, member, procedure, and service date",
    domain="claims",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    SELECT provider_npi, member_id, procedure_code, service_date,
           COUNT(*) AS claim_count,
           SUM(allowed_amount) AS total_allowed
    FROM claims
    WHERE provider_npi = %(provider_npi)s
      AND service_date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY provider_npi, member_id, procedure_code, service_date
    HAVING COUNT(*) > 1
    ORDER BY claim_count DESC
    """,
)

CLAIM_VOLUME_BY_PERIOD = SemanticConcept(
    name="claim_volume_by_period",
    description="Monthly claim counts and total allowed amounts for a provider",
    domain="claims",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    SELECT DATE_TRUNC('month', service_date) AS month,
           COUNT(*) AS claim_count,
           SUM(allowed_amount) AS total_allowed,
           AVG(allowed_amount) AS avg_allowed
    FROM claims
    WHERE provider_npi = %(provider_npi)s
      AND service_date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY DATE_TRUNC('month', service_date)
    ORDER BY month
    """,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PROVIDER DOMAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROVIDER_PEER_COMPARISON = SemanticConcept(
    name="provider_peer_comparison",
    description="Provider billing metrics vs peer cohort (same specialty, same state) with z-scores",
    domain="provider",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    WITH provider_metrics AS (
        SELECT c.provider_npi,
               COUNT(*)                    AS total_claims,
               SUM(c.allowed_amount)       AS total_allowed,
               AVG(c.allowed_amount)       AS avg_allowed,
               COUNT(DISTINCT c.member_id) AS unique_patients
        FROM claims c
        WHERE c.service_date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY c.provider_npi
    ),
    peer_stats AS (
        SELECT AVG(pm.total_claims)    AS mean_claims,
               STDDEV(pm.total_claims) AS std_claims,
               AVG(pm.total_allowed)   AS mean_allowed,
               STDDEV(pm.total_allowed) AS std_allowed
        FROM provider_metrics pm
        JOIN providers p ON pm.provider_npi = p.npi
        WHERE p.specialty = (SELECT specialty FROM providers WHERE npi = %(provider_npi)s)
          AND p.practice_state = (SELECT practice_state FROM providers WHERE npi = %(provider_npi)s)
    )
    SELECT pm.provider_npi, pm.total_claims, pm.total_allowed,
           pm.avg_allowed, pm.unique_patients,
           ROUND((pm.total_claims - ps.mean_claims) / NULLIF(ps.std_claims, 0), 2) AS z_claims,
           ROUND((pm.total_allowed - ps.mean_allowed) / NULLIF(ps.std_allowed, 0), 2) AS z_allowed
    FROM provider_metrics pm
    CROSS JOIN peer_stats ps
    WHERE pm.provider_npi = %(provider_npi)s
    """,
)

PROVIDER_SPECIALTY_MISMATCH = SemanticConcept(
    name="provider_specialty_mismatch",
    description="Claims for procedure codes outside the provider's credentialed specialty",
    domain="provider",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    SELECT c.procedure_code, c.service_date, c.allowed_amount,
           p.specialty AS provider_specialty,
           sm.expected_specialty AS procedure_specialty
    FROM claims c
    JOIN providers p ON c.provider_npi = p.npi
    JOIN specialty_procedure_map sm ON c.procedure_code = sm.procedure_code
    WHERE c.provider_npi = %(provider_npi)s
      AND c.service_date BETWEEN %(start_date)s AND %(end_date)s
      AND p.specialty != sm.expected_specialty
    ORDER BY c.allowed_amount DESC
    """,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MEMBER DOMAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DOCTOR_SHOPPING = SemanticConcept(
    name="doctor_shopping",
    description="Members visiting 4+ unique providers for the same diagnosis within a 30-day window",
    domain="member",
    parameters=["start_date", "end_date", "provider_npi"],
    sql_template="""
    WITH provider_visits AS (
        SELECT c.member_id, c.diagnosis_code,
               c.service_date, c.provider_npi
        FROM claims c
        WHERE c.service_date BETWEEN %(start_date)s AND %(end_date)s
          AND c.provider_npi = %(provider_npi)s
    ),
    member_visits AS (
        SELECT pv.member_id, pv.diagnosis_code,
               pv.service_date,
               (SELECT COUNT(DISTINCT pv2.provider_npi)
                FROM provider_visits pv2
                WHERE pv2.member_id = pv.member_id
                  AND pv2.diagnosis_code = pv.diagnosis_code
                  AND pv2.service_date BETWEEN DATEADD('day', -30, pv.service_date) AND pv.service_date
               ) AS provider_count_30d
        FROM provider_visits pv
    )
    SELECT DISTINCT member_id, diagnosis_code, provider_count_30d
    FROM member_visits
    WHERE provider_count_30d >= 4
    ORDER BY provider_count_30d DESC
    """,
)

MEMBER_UTILIZATION_SPIKE = SemanticConcept(
    name="member_utilization_spike",
    description="Members whose monthly claim count exceeds 3x their rolling 6-month average",
    domain="member",
    parameters=["start_date", "end_date", "provider_npi"],
    sql_template="""
    WITH monthly AS (
        SELECT member_id,
               DATE_TRUNC('month', service_date) AS month,
               COUNT(*) AS monthly_claims
        FROM claims
        WHERE service_date BETWEEN DATEADD('month', -6, %(start_date)s) AND %(end_date)s
          AND provider_npi = %(provider_npi)s
        GROUP BY member_id, DATE_TRUNC('month', service_date)
    ),
    with_avg AS (
        SELECT *,
               AVG(monthly_claims) OVER (
                   PARTITION BY member_id ORDER BY month
                   ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
               ) AS avg_6m
        FROM monthly
    )
    SELECT member_id, month, monthly_claims, ROUND(avg_6m, 1) AS avg_6m,
           ROUND(monthly_claims / NULLIF(avg_6m, 0), 1) AS spike_ratio
    FROM with_avg
    WHERE month >= %(start_date)s
      AND monthly_claims > 3 * COALESCE(avg_6m, 0)
    ORDER BY spike_ratio DESC
    """,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ELIGIBILITY DOMAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COVERAGE_GAP_CLAIMS = SemanticConcept(
    name="coverage_gap_claims",
    description="Claims billed during periods with no active eligibility",
    domain="eligibility",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    SELECT c.claim_id, c.member_id, c.service_date,
           c.procedure_code, c.allowed_amount
    FROM claims c
    LEFT JOIN eligibility e
        ON c.member_id = e.member_id
        AND c.service_date BETWEEN e.coverage_start AND e.coverage_end
    WHERE c.provider_npi = %(provider_npi)s
      AND c.service_date BETWEEN %(start_date)s AND %(end_date)s
      AND e.member_id IS NULL
    ORDER BY c.allowed_amount DESC
    """,
)

RETROACTIVE_ELIGIBILITY = SemanticConcept(
    name="retroactive_eligibility",
    description="Eligibility records added retroactively near high-cost claim dates",
    domain="eligibility",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    SELECT e.member_id, e.coverage_start, e.coverage_end,
           e.created_date,
           DATEDIFF('day', e.coverage_start, e.created_date) AS days_retroactive,
           COUNT(c.claim_id) AS claims_in_period,
           SUM(c.allowed_amount) AS total_allowed
    FROM eligibility e
    JOIN claims c ON e.member_id = c.member_id
        AND c.service_date BETWEEN e.coverage_start AND e.coverage_end
    WHERE c.provider_npi = %(provider_npi)s
      AND c.service_date BETWEEN %(start_date)s AND %(end_date)s
      AND e.created_date > e.coverage_start
      AND DATEDIFF('day', e.coverage_start, e.created_date) > 30
    GROUP BY e.member_id, e.coverage_start, e.coverage_end, e.created_date
    ORDER BY days_retroactive DESC
    """,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  TEMPORAL / CROSS-DOMAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BILLING_SPIKE_DETECTION = SemanticConcept(
    name="billing_spike_detection",
    description="Detects months where billing exceeds 2 std deviations from rolling average",
    domain="temporal",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    WITH monthly AS (
        SELECT DATE_TRUNC('month', service_date) AS month,
               COUNT(*) AS claims,
               SUM(allowed_amount) AS total_allowed
        FROM claims
        WHERE provider_npi = %(provider_npi)s
          AND service_date BETWEEN DATEADD('year', -2, %(start_date)s) AND %(end_date)s
        GROUP BY DATE_TRUNC('month', service_date)
    ),
    stats AS (
        SELECT *,
               AVG(total_allowed) OVER (ORDER BY month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS rolling_avg,
               STDDEV(total_allowed) OVER (ORDER BY month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING) AS rolling_std
        FROM monthly
    )
    SELECT month, claims, total_allowed,
           ROUND(rolling_avg, 2) AS rolling_avg,
           ROUND(rolling_std, 2) AS rolling_std,
           ROUND((total_allowed - rolling_avg) / NULLIF(rolling_std, 0), 2) AS z_score
    FROM stats
    WHERE month >= %(start_date)s
      AND (total_allowed - rolling_avg) / NULLIF(rolling_std, 0) > 2
    ORDER BY z_score DESC
    """,
)

WEEKEND_BILLING = SemanticConcept(
    name="weekend_billing",
    description="Claims submitted on weekends or holidays as a percentage of total",
    domain="temporal",
    parameters=["provider_npi", "start_date", "end_date"],
    sql_template="""
    SELECT DAYOFWEEK(service_date) AS dow,
           CASE DAYOFWEEK(service_date)
               WHEN 0 THEN 'Sunday'  WHEN 1 THEN 'Monday'
               WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday'
               WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
               WHEN 6 THEN 'Saturday'
           END AS day_name,
           COUNT(*) AS claim_count,
           SUM(allowed_amount) AS total_allowed
    FROM claims
    WHERE provider_npi = %(provider_npi)s
      AND service_date BETWEEN %(start_date)s AND %(end_date)s
    GROUP BY DAYOFWEEK(service_date)
    ORDER BY dow
    """,
)


# ── Registry ──
CONCEPT_REGISTRY: dict[str, SemanticConcept] = {
    c.name: c
    for c in [
        HIGH_COST_CLAIMS,
        UPCODING_PATTERN,
        DUPLICATE_CLAIMS,
        CLAIM_VOLUME_BY_PERIOD,
        PROVIDER_PEER_COMPARISON,
        PROVIDER_SPECIALTY_MISMATCH,
        DOCTOR_SHOPPING,
        MEMBER_UTILIZATION_SPIKE,
        COVERAGE_GAP_CLAIMS,
        RETROACTIVE_ELIGIBILITY,
        BILLING_SPIKE_DETECTION,
        WEEKEND_BILLING,
    ]
}


def get_concepts_for_domain(domain: str) -> list[SemanticConcept]:
    """Return all concepts belonging to a domain."""
    return [c for c in CONCEPT_REGISTRY.values() if c.domain == domain]


def get_concept(name: str) -> SemanticConcept | None:
    return CONCEPT_REGISTRY.get(name)
