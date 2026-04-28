"""Generate a PDF report from investigation results."""

from __future__ import annotations

import io
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)


# ── Colour palette ──────────────────────────────────────────────────
_DARK = colors.HexColor("#111827")
_ACCENT = colors.HexColor("#3b82f6")
_RED = colors.HexColor("#ef4444")
_ORANGE = colors.HexColor("#f59e0b")
_GREEN = colors.HexColor("#22c55e")
_GREY = colors.HexColor("#64748b")
_LIGHT_BG = colors.HexColor("#f1f5f9")

_SEVERITY_COLORS = {
    "critical": _RED,
    "high": _ORANGE,
    "medium": _ORANGE,
    "low": _GREEN,
    "info": _ACCENT,
}


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "PDFTitle", parent=base["Title"],
            fontSize=22, leading=26, textColor=_DARK, spaceAfter=4,
        ),
        "subtitle": ParagraphStyle(
            "PDFSubtitle", parent=base["Normal"],
            fontSize=11, leading=14, textColor=_GREY, spaceAfter=16,
        ),
        "h2": ParagraphStyle(
            "PDFH2", parent=base["Heading2"],
            fontSize=14, leading=18, textColor=_DARK, spaceBefore=18, spaceAfter=8,
        ),
        "body": ParagraphStyle(
            "PDFBody", parent=base["Normal"],
            fontSize=10, leading=14, textColor=_DARK,
        ),
        "finding_title": ParagraphStyle(
            "PDFFindingTitle", parent=base["Normal"],
            fontSize=10, leading=13, textColor=_DARK, fontName="Helvetica-Bold",
        ),
        "finding_detail": ParagraphStyle(
            "PDFFindingDetail", parent=base["Normal"],
            fontSize=9, leading=12, textColor=_GREY,
        ),
        "severity": ParagraphStyle(
            "PDFSeverity", parent=base["Normal"],
            fontSize=9, leading=11, fontName="Helvetica-Bold",
        ),
    }


def _fmt_money(n: float | int) -> str:
    if not n:
        return "$0"
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n / 1_000:,.0f}K"
    return f"${n:,.0f}"


def generate_pdf(data: dict[str, Any]) -> bytes:
    """Return PDF bytes for an investigation result dict."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=letter,
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.6 * inch, bottomMargin=0.6 * inch,
    )
    s = _styles()
    story: list = []

    report = data.get("report") or {}
    agent_results = data.get("agent_results") or {}
    query = data.get("query", "")
    inv_id = data.get("id", "N/A")
    elapsed = data.get("elapsed_seconds", 0)

    # ── Header ──────────────────────────────────────────────────────
    story.append(Paragraph("Healthcare Investigation Report", s["title"]))
    story.append(Paragraph(
        f"Investigation {inv_id} &bull; {elapsed:.1f}s &bull; "
        f"{sum(len((ar.get('findings') or []) ) for ar in agent_results.values())} findings",
        s["subtitle"],
    ))
    story.append(HRFlowable(width="100%", thickness=1, color=_ACCENT, spaceAfter=12))

    # ── Query ───────────────────────────────────────────────────────
    story.append(Paragraph("Investigation Query", s["h2"]))
    story.append(Paragraph(query, s["body"]))
    story.append(Spacer(1, 10))

    # ── Stats table ─────────────────────────────────────────────────
    risk_level = (report.get("risk_level") or "N/A").upper()
    risk_score = report.get("composite_risk_score") or 0
    financial = report.get("financial_summary") or {}
    total_overpayment = financial.get("total_estimated_overpayment", 0)

    total_findings = 0
    for ar in agent_results.values():
        total_findings += len(ar.get("findings") or [])

    stats_data = [
        ["Risk Level", "Risk Score", "Findings", "Est. Impact"],
        [risk_level, f"{risk_score * 100:.0f}%", str(total_findings), _fmt_money(total_overpayment)],
    ]
    stats_table = Table(stats_data, colWidths=[1.7 * inch] * 4)
    stats_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), _ACCENT),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("BACKGROUND", (0, 1), (-1, 1), _LIGHT_BG),
        ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 1), (-1, 1), 13),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("BOX", (0, 0), (-1, -1), 1, _ACCENT),
    ]))
    story.append(stats_table)
    story.append(Spacer(1, 16))

    # ── Executive Summary ───────────────────────────────────────────
    exec_summary = report.get("executive_summary", "No executive summary available.")
    story.append(Paragraph("Executive Summary", s["h2"]))
    story.append(Paragraph(exec_summary, s["body"]))
    story.append(Spacer(1, 8))

    # ── Key Findings ────────────────────────────────────────────────
    story.append(Paragraph("Key Findings", s["h2"]))

    all_findings: list[dict] = []
    if report.get("key_findings"):
        for f in report["key_findings"]:
            all_findings.append({
                "severity": f.get("severity", "info"),
                "title": f.get("title", ""),
                "explanation": f.get("description", ""),
                "agent": ", ".join(f.get("supporting_agents") or []),
                "impact": f.get("estimated_impact_usd"),
            })
    else:
        for agent_id, ar in agent_results.items():
            for f in (ar.get("findings") or []):
                all_findings.append({
                    "severity": f.get("severity", "info"),
                    "title": f.get("title", ""),
                    "explanation": f.get("explanation", ""),
                    "agent": agent_id,
                })

    if all_findings:
        for f in all_findings:
            sev = f["severity"]
            sev_color = _SEVERITY_COLORS.get(sev, _GREY)
            title_text = (
                f'<font color="{sev_color.hexval()}">[{sev.upper()}]</font> '
                f'{f["title"]}'
            )
            story.append(Paragraph(title_text, s["finding_title"]))
            if f.get("explanation"):
                story.append(Paragraph(f["explanation"], s["finding_detail"]))
            meta_parts = []
            if f.get("agent"):
                meta_parts.append(f"Source: {f['agent']}")
            if f.get("impact"):
                meta_parts.append(f"Impact: {_fmt_money(f['impact'])}")
            if meta_parts:
                story.append(Paragraph(" · ".join(meta_parts), s["finding_detail"]))
            story.append(Spacer(1, 6))
    else:
        story.append(Paragraph("No findings recorded.", s["body"]))

    # ── Recommended Actions ─────────────────────────────────────────
    actions = report.get("recommended_actions") or []
    if actions:
        story.append(Paragraph("Recommended Actions", s["h2"]))
        action_data = [["Priority", "Action", "Assigned To"]]
        for a in actions:
            action_data.append([
                f"P{a.get('priority', '?')}",
                a.get("action", ""),
                a.get("assigned_to", ""),
            ])
        action_table = Table(action_data, colWidths=[0.7 * inch, 4.5 * inch, 1.6 * inch])
        action_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), _ACCENT),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ALIGN", (0, 0), (0, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _LIGHT_BG]),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
            ("BOX", (0, 0), (-1, -1), 1, _ACCENT),
        ]))
        story.append(action_table)
        story.append(Spacer(1, 12))

    # ── Agent Execution Summary ─────────────────────────────────────
    story.append(Paragraph("Agent Execution Summary", s["h2"]))
    agent_data = [["Agent", "Status", "Confidence", "Findings", "Time (s)"]]
    for agent_id, ar in agent_results.items():
        agent_data.append([
            ar.get("agent_name", agent_id),
            ar.get("status", ""),
            f"{ar.get('confidence', 0):.0%}",
            str(len(ar.get("findings") or [])),
            f"{ar.get('execution_time', 0):.1f}",
        ])
    agent_table = Table(agent_data, colWidths=[1.8 * inch, 0.9 * inch, 1.0 * inch, 0.9 * inch, 0.9 * inch])
    agent_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), _DARK),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, _LIGHT_BG]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
        ("BOX", (0, 0), (-1, -1), 1, _DARK),
    ]))
    story.append(agent_table)

    # ── Footer ──────────────────────────────────────────────────────
    story.append(Spacer(1, 24))
    story.append(HRFlowable(width="100%", thickness=0.5, color=_GREY, spaceAfter=6))
    story.append(Paragraph(
        "Generated by Healthcare AI Investigation Engine · Multi-Agent Orchestration System",
        ParagraphStyle("footer", parent=s["body"], fontSize=8, textColor=_GREY, alignment=1),
    ))

    doc.build(story)
    return buf.getvalue()
