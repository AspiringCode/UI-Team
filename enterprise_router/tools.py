from __future__ import annotations
import json
from datetime import datetime
from typing import Dict, Any, List


_BACKLOG: List[Dict[str, Any]] = [
    {"id": "PROJ-101", "title": "Auth service rate limiting",       "impact": 9, "effort": 3, "status": "ready",       "owner": None},
    {"id": "PROJ-102", "title": "Dashboard load time > 4s",         "impact": 8, "effort": 5, "status": "ready",       "owner": None},
    {"id": "PROJ-103", "title": "Mobile push notifications",        "impact": 6, "effort": 6, "status": "in_progress", "owner": "worker_agent_01"},
    {"id": "PROJ-104", "title": "CSV export for reports",           "impact": 5, "effort": 2, "status": "ready",       "owner": None},
    {"id": "PROJ-105", "title": "SSO integration",                  "impact": 9, "effort": 8, "status": "blocked",     "owner": None},
    {"id": "PROJ-106", "title": "Onboarding email sequence",        "impact": 4, "effort": 2, "status": "ready",       "owner": None},
    {"id": "PROJ-107", "title": "API pagination bug",               "impact": 7, "effort": 1, "status": "ready",       "owner": None},
    {"id": "PROJ-108", "title": "GDPR data deletion endpoint",      "impact": 8, "effort": 4, "status": "ready",       "owner": None},
]

_BUDGET: Dict[str, Any] = {
    "q_current": "Q2-2025",
    "total_budget": 2_400_000,
    "spent": 1_105_000,
    "departments": {
        "engineering": {"budget": 900_000,  "spent": 480_000},
        "marketing":   {"budget": 600_000,  "spent": 320_000},
        "sales":       {"budget": 550_000,  "spent": 210_000},
        "hr":          {"budget": 350_000,  "spent": 95_000},
    },
    "pending_requests": [
        {"id": "BUD-201", "dept": "engineering", "amount": 45_000,  "description": "3x contractor licenses Q2",  "roi_estimate": 2.1},
        {"id": "BUD-202", "dept": "marketing",   "amount": 80_000,  "description": "Paid acquisition campaign",  "roi_estimate": 3.4},
        {"id": "BUD-203", "dept": "engineering", "amount": 120_000, "description": "Cloud infra scaling",         "roi_estimate": 1.4},
        {"id": "BUD-204", "dept": "sales",       "amount": 30_000,  "description": "Conference sponsorship",     "roi_estimate": 1.9},
    ],
}

_CONTENT_BRIEFS: List[Dict[str, Any]] = [
    {"id": "CNT-301", "channel": "blog",     "topic": "How we cut API latency by 60%",          "target_audience": "developers",    "status": "draft"},
    {"id": "CNT-302", "channel": "linkedin", "topic": "Product update: new dashboard",           "target_audience": "decision_makers","status": "approved"},
    {"id": "CNT-303", "channel": "email",    "topic": "Q2 feature roundup newsletter",           "target_audience": "existing_users", "status": "draft"},
    {"id": "CNT-304", "channel": "blog",     "topic": "5 ways teams use our API",                "target_audience": "developers",    "status": "needs_review"},
    {"id": "CNT-305", "channel": "twitter",  "topic": "Auth rate limiting launch announcement",  "target_audience": "developers",    "status": "draft"},
]


def _fmt_money(v: int) -> str:
    return f"${v:,.0f}"


class PMTools:
    MIN_PRIORITY_SCORE = 5.0

    @staticmethod
    def score(item: Dict) -> float:
        return round(item["impact"] / max(item["effort"], 1), 2)

    def read_backlog(self, payload: Dict) -> Dict[str, Any]:
        status_filter = payload.get("status_filter")
        items = [i for i in _BACKLOG if not status_filter or i["status"] == status_filter]
        scored = sorted(
            [dict(i, priority_score=self.score(i)) for i in items],
            key=lambda x: x["priority_score"],
            reverse=True,
        )
        return {
            "total": len(scored),
            "filter": status_filter or "all",
            "items": scored,
        }

    def prioritize_backlog(self, payload: Dict) -> Dict[str, Any]:
        ready = [i for i in _BACKLOG if i["status"] == "ready"]
        scored = sorted(
            [dict(i, priority_score=self.score(i)) for i in ready],
            key=lambda x: x["priority_score"],
            reverse=True,
        )
        selected = [i for i in scored if i["priority_score"] >= self.MIN_PRIORITY_SCORE]
        skipped  = [i for i in scored if i["priority_score"] < self.MIN_PRIORITY_SCORE]
        decision = (
            f"Selecting top {len(selected)} items with priority_score >= {self.MIN_PRIORITY_SCORE} "
            f"(impact/effort). Skipping {len(skipped)} below threshold."
        )
        return {
            "decision_rule": f"priority_score = impact / effort, threshold = {self.MIN_PRIORITY_SCORE}",
            "decision": decision,
            "sprint_candidates": selected,
            "skipped_low_priority": skipped,
        }

    def assign_task(self, payload: Dict) -> Dict[str, Any]:
        task_id = payload.get("task_id")
        assignee = payload.get("assignee", "worker_agent_01")
        item = next((i for i in _BACKLOG if i["id"] == task_id), None)
        if not item:
            return {"error": f"Task {task_id!r} not found in backlog."}
        if item["status"] == "blocked":
            return {"error": f"Task {task_id!r} is blocked and cannot be assigned."}
        item["owner"] = assignee
        item["status"] = "in_progress"
        return {
            "task_id": task_id,
            "title": item["title"],
            "assigned_to": assignee,
            "status": "in_progress",
        }


class FinanceTools:
    MIN_ROI = 2.0

    def budget_summary(self, payload: Dict) -> Dict[str, Any]:
        remaining = _BUDGET["total_budget"] - _BUDGET["spent"]
        utilization = round(_BUDGET["spent"] / _BUDGET["total_budget"] * 100, 1)
        dept_rows = []
        for dept, d in _BUDGET["departments"].items():
            dept_remaining = d["budget"] - d["spent"]
            dept_util = round(d["spent"] / d["budget"] * 100, 1)
            dept_rows.append({
                "department": dept,
                "budget": _fmt_money(d["budget"]),
                "spent": _fmt_money(d["spent"]),
                "remaining": _fmt_money(dept_remaining),
                "utilization_pct": dept_util,
            })
        return {
            "quarter": _BUDGET["q_current"],
            "total_budget": _fmt_money(_BUDGET["total_budget"]),
            "total_spent": _fmt_money(_BUDGET["spent"]),
            "remaining": _fmt_money(remaining),
            "utilization_pct": utilization,
            "by_department": dept_rows,
        }

    def evaluate_spend_requests(self, payload: Dict) -> Dict[str, Any]:
        results = []
        for req in _BUDGET["pending_requests"]:
            dept = _BUDGET["departments"].get(req["dept"], {})
            dept_remaining = dept.get("budget", 0) - dept.get("spent", 0)
            roi_ok = req["roi_estimate"] >= self.MIN_ROI
            budget_ok = req["amount"] <= dept_remaining
            approved = roi_ok and budget_ok
            reason_parts = []
            if not roi_ok:
                reason_parts.append(f"ROI {req['roi_estimate']}x below minimum {self.MIN_ROI}x")
            if not budget_ok:
                reason_parts.append(f"amount {_fmt_money(req['amount'])} exceeds dept remaining {_fmt_money(dept_remaining)}")
            results.append({
                "id": req["id"],
                "dept": req["dept"],
                "amount": _fmt_money(req["amount"]),
                "description": req["description"],
                "roi_estimate": f"{req['roi_estimate']}x",
                "decision": "approved" if approved else "rejected",
                "reason": "Meets ROI and budget criteria." if approved else "; ".join(reason_parts),
            })
        approved_count = sum(1 for r in results if r["decision"] == "approved")
        return {
            "decision_rule": f"Approve if ROI >= {self.MIN_ROI}x AND amount <= dept remaining budget",
            "total_evaluated": len(results),
            "approved": approved_count,
            "rejected": len(results) - approved_count,
            "evaluations": results,
        }

    def forecast_runway(self, payload: Dict) -> Dict[str, Any]:
        months_elapsed = payload.get("months_elapsed", 1)
        monthly_burn = _BUDGET["spent"] / max(months_elapsed, 1)
        remaining = _BUDGET["total_budget"] - _BUDGET["spent"]
        months_remaining = round(remaining / monthly_burn, 1) if monthly_burn else 0
        on_track = monthly_burn * 3 <= _BUDGET["total_budget"]
        return {
            "monthly_burn_rate": _fmt_money(int(monthly_burn)),
            "remaining_budget": _fmt_money(remaining),
            "estimated_months_remaining": months_remaining,
            "on_track_for_quarter": on_track,
            "recommendation": (
                "Budget on track." if on_track
                else "Burn rate exceeds plan. Flag for CEO review."
            ),
        }


class MarketingTools:
    APPROVED_CHANNELS = {"blog", "linkedin", "email"}

    def list_content_briefs(self, payload: Dict) -> Dict[str, Any]:
        channel = payload.get("channel")
        items = [b for b in _CONTENT_BRIEFS if not channel or b["channel"] == channel]
        return {
            "total": len(items),
            "channel_filter": channel or "all",
            "briefs": items,
        }

    def generate_content_brief(self, payload: Dict) -> Dict[str, Any]:
        topic = payload.get("topic", "Product Update")
        channel = payload.get("channel", "blog")
        audience = payload.get("target_audience", "general")

        if channel not in self.APPROVED_CHANNELS:
            return {
                "error": f"Channel '{channel}' not in approved list: {sorted(self.APPROVED_CHANNELS)}. Content not generated.",
                "decision_rule": f"Only publish to approved channels: {sorted(self.APPROVED_CHANNELS)}",
            }

        formats = {
            "blog":     {"word_count": "800–1200 words", "tone": "technical and informative", "cta": "Try it in your project"},
            "linkedin": {"word_count": "150–300 words",  "tone": "professional and concise",  "cta": "Learn more in the comments"},
            "email":    {"word_count": "200–400 words",  "tone": "friendly and direct",        "cta": "See what's new"},
        }
        fmt = formats.get(channel, formats["blog"])
        new_id = f"CNT-{300 + len(_CONTENT_BRIEFS) + 1}"
        brief = {
            "id": new_id,
            "channel": channel,
            "topic": topic,
            "target_audience": audience,
            "word_count": fmt["word_count"],
            "tone": fmt["tone"],
            "suggested_cta": fmt["cta"],
            "key_messages": [
                f"Highlight the core value of: {topic}",
                f"Address pain points for {audience}",
                "Include one concrete metric or example",
            ],
            "status": "draft",
        }
        _CONTENT_BRIEFS.append(brief)
        return {
            "decision_rule": f"Only generate for approved channels: {sorted(self.APPROVED_CHANNELS)}",
            "brief": brief,
        }

    def review_content(self, payload: Dict) -> Dict[str, Any]:
        content_id = payload.get("content_id")
        item = next((b for b in _CONTENT_BRIEFS if b["id"] == content_id), None)
        if not item:
            return {"error": f"Content {content_id!r} not found."}
        issues = []
        if item["channel"] not in self.APPROVED_CHANNELS:
            issues.append(f"Channel '{item['channel']}' not approved for publishing.")
        if item["status"] == "draft":
            issues.append("Still in draft — needs copywriter sign-off.")
        item["status"] = "approved" if not issues else "needs_review"
        return {
            "content_id": content_id,
            "channel": item["channel"],
            "topic": item["topic"],
            "review_status": item["status"],
            "issues": issues,
            "cleared_for_publish": not issues,
        }


class CEOTools:
    MIN_ROI = 2.0
    CRITICAL_BUDGET_UTIL = 85.0

    def review_spend_approvals(self, payload: Dict) -> Dict[str, Any]:
        finance_eval = payload.get("finance_evaluation", {})
        evaluations = finance_eval.get("evaluations", [])
        if not evaluations:
            return {"error": "No finance evaluation provided. Run finance budget_evaluate first."}

        ceo_decisions = []
        for ev in evaluations:
            roi_str = ev.get("roi_estimate", "0x").replace("x", "")
            try:
                roi = float(roi_str)
            except ValueError:
                roi = 0.0
            finance_approved = ev.get("decision") == "approved"
            override = False
            override_reason = ""
            if finance_approved and roi < self.MIN_ROI:
                override = True
                override_reason = f"CEO veto: ROI {roi}x below minimum {self.MIN_ROI}x."
            ceo_decisions.append({
                "id": ev["id"],
                "description": ev.get("description", ""),
                "finance_decision": ev.get("decision"),
                "ceo_decision": "rejected" if override else ev.get("decision"),
                "override": override,
                "note": override_reason if override else ev.get("reason", ""),
            })

        final_approved = sum(1 for d in ceo_decisions if d["ceo_decision"] == "approved")
        return {
            "decision_rule": f"CEO enforces minimum ROI >= {self.MIN_ROI}x on all spend requests",
            "total": len(ceo_decisions),
            "final_approved": final_approved,
            "final_rejected": len(ceo_decisions) - final_approved,
            "decisions": ceo_decisions,
        }

    def set_quarterly_priorities(self, payload: Dict) -> Dict[str, Any]:
        focus_areas = payload.get("focus_areas", ["revenue", "reliability", "retention"])
        directives = []
        for area in focus_areas:
            directive_map = {
                "revenue":     "All sprint work must map to a revenue or activation metric.",
                "reliability": "P0/P1 bugs take precedence over all feature work.",
                "retention":   "Churn-risk features must be unblocked within 2 sprints.",
                "growth":      "Marketing spend requires CEO sign-off above $50k.",
                "hiring":      "All senior hires require CEO panel interview.",
            }
            directives.append({
                "area": area,
                "directive": directive_map.get(area, f"Teams must report weekly on {area} metrics."),
            })
        return {
            "quarter": "Q2-2025",
            "focus_areas": focus_areas,
            "directives": directives,
            "communicated_to": ["pm_agent", "finance_agent", "marketing_agent"],
        }

    def check_budget_health(self, payload: Dict) -> Dict[str, Any]:
        util = round(_BUDGET["spent"] / _BUDGET["total_budget"] * 100, 1)
        alert = util >= self.CRITICAL_BUDGET_UTIL
        dept_alerts = []
        for dept, d in _BUDGET["departments"].items():
            du = round(d["spent"] / d["budget"] * 100, 1)
            if du >= self.CRITICAL_BUDGET_UTIL:
                dept_alerts.append({"department": dept, "utilization_pct": du})
        return {
            "overall_utilization_pct": util,
            "alert": alert,
            "alert_threshold_pct": self.CRITICAL_BUDGET_UTIL,
            "department_alerts": dept_alerts,
            "action_required": alert or bool(dept_alerts),
            "recommendation": (
                "Freeze discretionary spend and escalate to CEO." if alert
                else "Budget within acceptable range."
            ),
        }