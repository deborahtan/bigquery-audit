"""
Prebaked report generator for BigQuery audits
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import streamlit as st
from .bigquery_connector import WoolworthsBigQueryConnector
from .anomaly_detector import RetailAnomalyDetector


class WoolworthsReportGenerator:
    """
    Generate comprehensive audit reports for Woolworths GA4 data
    """
    
    def __init__(self, bq_connector: WoolworthsBigQueryConnector):
        self.bq = bq_connector
        self.anomaly_detector = RetailAnomalyDetector()
    
    def generate_daily_audit_report(self, report_date: datetime.date = None) -> Dict:
        """
        Generate full daily audit report
        
        Returns:
            Dict with all audit sections and health score
        """
        if report_date is None:
            report_date = datetime.now().date() - timedelta(days=1)
        
        report = {
            "report_date": report_date,
            "generated_at": datetime.now(),
            "sections": {},
            "cache_performance": {}
        }
        
        # 1. Volume & Spikes
        volume_data, cache_src = self.bq.get_daily_spikes(days_back=14, threshold=2.0)
        report["sections"]["volume"] = {
            "data": volume_data,
            "cache_source": cache_src,
            "summary": self._summarize_volume(volume_data)
        }
        report["cache_performance"]["volume"] = cache_src
        
        # 2. Event Drop-offs
        dropoff_data, cache_src = self.bq.get_event_dropoffs(days_back=14)
        report["sections"]["dropoffs"] = {
            "data": dropoff_data,
            "cache_source": cache_src,
            "summary": self._summarize_dropoffs(dropoff_data)
        }
        report["cache_performance"]["dropoffs"] = cache_src
        
        # 3. Null Rates
        null_data, cache_src = self.bq.get_null_rates_per_event(days_back=7)
        report["sections"]["null_rates"] = {
            "data": null_data,
            "cache_source": cache_src,
            "summary": self._summarize_null_rates(null_data)
        }
        report["cache_performance"]["null_rates"] = cache_src
        
        # 4. Ecommerce Validation
        ecommerce_data, cache_src = self.bq.get_ecommerce_validation(days_back=7)
        report["sections"]["ecommerce"] = {
            "data": ecommerce_data,
            "cache_source": cache_src,
            "summary": self._summarize_ecommerce(ecommerce_data)
        }
        report["cache_performance"]["ecommerce"] = cache_src
        
        # 5. Store & Loyalty Tracking
        store_data, cache_src = self.bq.get_store_tracking_health(days_back=7)
        report["sections"]["store_loyalty"] = {
            "data": store_data,
            "cache_source": cache_src,
            "summary": self._summarize_store_loyalty(store_data)
        }
        report["cache_performance"]["store_loyalty"] = cache_src
        
        # 6. Data Freshness
        freshness_data, cache_src = self.bq.get_data_freshness()
        report["sections"]["freshness"] = {
            "data": freshness_data,
            "cache_source": cache_src,
            "summary": self._summarize_freshness(freshness_data)
        }
        report["cache_performance"]["freshness"] = cache_src
        
        # 7. Traffic Source Health
        traffic_data, cache_src = self.bq.get_traffic_source_health(days_back=7)
        report["sections"]["traffic_source"] = {
            "data": traffic_data,
            "cache_source": cache_src,
            "summary": self._summarize_traffic_source(traffic_data)
        }
        report["cache_performance"]["traffic_source"] = cache_src
        
        # Calculate overall health score
        report["health_score"] = self._calculate_health_score(report)
        report["critical_issues"] = self._extract_critical_issues(report)
        report["recommendations"] = self._generate_recommendations(report)
        
        return report
    
    def _summarize_volume(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "✅ No significant volume anomalies detected"
        
        high_spikes = data[data['severity'] == 'HIGH']
        if not high_spikes.empty:
            return f"🔴 {len(high_spikes)} HIGH severity spike(s) detected"
        
        medium_spikes = data[data['severity'] == 'MEDIUM']
        if not medium_spikes.empty:
            return f"🟡 {len(medium_spikes)} MEDIUM severity spike(s) detected"
        
        return "✅ Traffic volume within normal range"
    
    def _summarize_dropoffs(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "✅ All events firing normally"
        
        critical = data[data['alert_level'].str.contains('CRITICAL', na=False)]
        if not critical.empty:
            return f"🔴 {len(critical)} event(s) stopped firing or dropped >50%"
        
        return f"🟡 {len(data)} event(s) with volume drops >30%"
    
    def _summarize_null_rates(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "✅ Null rates within acceptable range"
        
        critical = data[data['alert_status'].str.contains('CRITICAL', na=False)]
        if not critical.empty:
            return f"🔴 {len(critical)} event(s) with CRITICAL null rates"
        
        warnings = data[data['alert_status'].str.contains('WARNING', na=False)]
        if not warnings.empty:
            return f"🟡 {len(warnings)} event(s) with elevated null rates"
        
        return "✅ Null rates within acceptable range"
    
    def _summarize_ecommerce(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "💡 No ecommerce events to validate"
        
        critical = data[data['validation_status'].str.contains('CRITICAL', na=False)]
        if not critical.empty:
            return f"🔴 {len(critical)} ecommerce event(s) with CRITICAL issues"
        
        warnings = data[data['validation_status'].str.contains('WARNING', na=False)]
        if not warnings.empty:
            return f"🟡 {len(warnings)} ecommerce event(s) with warnings"
        
        return "✅ Ecommerce tracking healthy"
    
    def _summarize_store_loyalty(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "💡 No store/loyalty data to check"
        
        warnings = data[data['tracking_status'].str.contains('WARNING', na=False)]
        if not warnings.empty:
            return f"🟡 Store/loyalty tracking needs improvement"
        
        return "✅ Store and loyalty tracking healthy"
    
    def _summarize_freshness(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "💡 Unable to check data freshness"
        
        status = data['freshness_status'].iloc[0] if 'freshness_status' in data.columns else '✅ Fresh data'
        return status
    
    def _summarize_traffic_source(self, data: pd.DataFrame) -> str:
        if data.empty:
            return "💡 No traffic source data to check"
        
        critical = data[data['tracking_status'].str.contains('CRITICAL', na=False)]
        if not critical.empty:
            return f"🔴 Traffic source tracking has CRITICAL issues"
        
        warnings = data[data['tracking_status'].str.contains('WARNING', na=False)]
        if not warnings.empty:
            return f"🟡 Traffic source tracking needs attention"
        
        return "✅ Traffic source tracking healthy"
    
    def _calculate_health_score(self, report: Dict) -> int:
        """Calculate overall health score (0-100)"""
        score = 100
        sections = report["sections"]
        
        # Deduct points for issues
        if "volume" in sections and not sections["volume"]["data"].empty:
            high_spikes = len(sections["volume"]["data"][sections["volume"]["data"]["severity"] == "HIGH"])
            score -= high_spikes * 10
        
        if "dropoffs" in sections:
            score -= len(sections["dropoffs"]["data"]) * 15
        
        if "null_rates" in sections:
            null_data = sections["null_rates"]["data"]
            critical = len(null_data[null_data['alert_status'].str.contains('CRITICAL', na=False)])
            warnings = len(null_data[null_data['alert_status'].str.contains('WARNING', na=False)])
            score -= (critical * 20 + warnings * 10)
        
        if "ecommerce" in sections:
            ecom_data = sections["ecommerce"]["data"]
            critical = len(ecom_data[ecom_data['validation_status'].str.contains('CRITICAL', na=False)])
            score -= critical * 15
        
        return max(0, min(100, score))
    
    def _extract_critical_issues(self, report: Dict) -> List[Dict]:
        """Extract all critical issues"""
        issues = []
        
        for section_name, section_data in report["sections"].items():
            summary = section_data["summary"]
            if "🔴" in summary:
                issues.append({
                    "category": section_name,
                    "severity": "CRITICAL",
                    "description": summary,
                    "data_preview": section_data["data"].head(3).to_dict('records') if not section_data["data"].empty else []
                })
        
        return issues
    
    def _generate_recommendations(self, report: Dict) -> List[Dict]:
        """Generate actionable recommendations"""
        recommendations = []
        sections = report["sections"]
        
        # Check for drop-offs
        if "dropoffs" in sections and not sections["dropoffs"]["data"].empty:
            recommendations.append({
                "priority": "HIGH",
                "category": "Event Implementation",
                "action": "Check GTM configuration for missing or broken event triggers",
                "affected_events": sections["dropoffs"]["data"]["event_name"].tolist()[:5]
            })
        
        # Check for null rates
        if "null_rates" in sections:
            critical_nulls = sections["null_rates"]["data"][
                sections["null_rates"]["data"]["alert_status"].str.contains("CRITICAL", na=False)
            ]
            if not critical_nulls.empty:
                recommendations.append({
                    "priority": "HIGH",
                    "category": "Data Quality",
                    "action": "Investigate high null rates - check dataLayer implementation",
                    "affected_fields": critical_nulls[["event_name", "alert_status"]].head(3).to_dict('records')
                })
        
        # Check ecommerce
        if "ecommerce" in sections:
            critical_ecom = sections["ecommerce"]["data"][
                sections["ecommerce"]["data"]["validation_status"].str.contains("CRITICAL", na=False)
            ]
            if not critical_ecom.empty:
                recommendations.append({
                    "priority": "CRITICAL",
                    "category": "Revenue Tracking",
                    "action": "Fix ecommerce tracking immediately - revenue data is missing",
                    "impact": "Revenue reporting affected"
                })
        
        return recommendations
    
    def export_report_to_markdown(self, report: Dict, output_path: str = "audit_report.md") -> str:
        """Export report to Markdown file"""
        md = f"""# Woolworths NZ GA4 Audit Report
**Report Date:** {report['report_date']}  
**Generated:** {report['generated_at'].strftime('%Y-%m-%d %H:%M:%S')}  
**Health Score:** {report['health_score']}/100

## 🎯 Executive Summary

"""
        
        # Critical issues
        if report["critical_issues"]:
            md += "### 🔴 Critical Issues Requiring Immediate Attention\n\n"
            for issue in report["critical_issues"]:
                md += f"**{issue['category'].upper()}**: {issue['description']}\n\n"
        else:
            md += "✅ No critical issues detected\n\n"
        
        # Recommendations
        if report["recommendations"]:
            md += "### 💡 Recommended Actions\n\n"
            for rec in report["recommendations"]:
                md += f"**[{rec['priority']}] {rec['category']}**\n"
                md += f"- {rec['action']}\n\n"
        
        # Section details
        md += "## 📊 Detailed Findings\n\n"
        for section_name, section_data in report["sections"].items():
            cache_emoji = "💾" if section_data["cache_source"] == "disk" else "⚡" if section_data["cache_source"] == "memory" else "🔄"
            md += f"### {cache_emoji} {section_name.replace('_', ' ').title()}\n\n"
            md += f"{section_data['summary']}\n\n"
            if not section_data["data"].empty:
                md += section_data["data"].head(10).to_markdown(index=False)
                md += "\n\n"
        
        # Cache performance
        md += "## ⚡ Cache Performance\n\n"
        for section, source in report["cache_performance"].items():
            md += f"- {section}: {source}\n"
        
        with open(output_path, 'w') as f:
            f.write(md)
        
        return output_path
