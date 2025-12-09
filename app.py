"""
Woolworths NZ BigQuery Audit Dashboard
Dentsu Analytics Team
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv
from groq import Groq
import json
import tempfile

# Import our modules
from src.bigquery_connector import WoolworthsBigQueryConnector
from src.report_generator import WoolworthsReportGenerator
from src.anomaly_detector import RetailAnomalyDetector
from src.rag_system import SimpleRAGSystem

# Load environment variables (for local development)
load_dotenv()

# ------------------------------
# Credentials & Config Management
# ------------------------------

def get_credentials_path():
    """
    Get GCP credentials - try Streamlit secrets first, then environment
    Handles both Streamlit Cloud deployment and local development
    """
    # Check if running in Streamlit Cloud (has secrets)
    if hasattr(st, 'secrets'):
        try:
            # If GCP_CREDENTIALS exists in Streamlit secrets, use it
            if 'GCP_CREDENTIALS' in st.secrets:
                # Parse JSON credentials from secrets
                creds_dict = json.loads(st.secrets['GCP_CREDENTIALS'])
                
                # Create temporary file to store credentials
                temp_creds = tempfile.NamedTemporaryFile(
                    mode='w', 
                    delete=False, 
                    suffix='.json'
                )
                json.dump(creds_dict, temp_creds)
                temp_creds.close()
                
                return temp_creds.name
        except Exception as e:
            # If secrets parsing fails, show warning but continue
            st.sidebar.warning(f"Could not load Streamlit secrets: {e}")
    
    # Fall back to local .env file (for local development)
    local_path = os.getenv("GCP_CREDENTIALS_PATH")
    if local_path and os.path.exists(local_path):
        return local_path
    
    # No credentials found
    return None


def get_config_value(key, default=""):
    """
    Get config value - try Streamlit secrets first, then environment
    Supports both deployment and local development
    """
    # Try Streamlit secrets first (cloud deployment)
    if hasattr(st, 'secrets') and key in st.secrets:
        return st.secrets[key]
    
    # Fall back to environment variables (local development)
    return os.getenv(key, default)


# ------------------------------
# Page Configuration & Styling
# ------------------------------

st.set_page_config(
    page_title="Woolworths NZ BigQuery Audit Dashboard",
    page_icon="🔍",
    layout="wide"
)

# Dentsu Styling
st.markdown(
    """
    <style>
    @font-face {
        font-family: 'StabilGrotesk';
        src: url('app/static/StabilGrotesk-Regular.otf') format('opentype');
        font-weight: 400;
        font-style: normal;
    }
    @font-face {
        font-family: 'StabilGrotesk';
        src: url('app/static/StabilGrotesk-Bold.otf') format('opentype');
        font-weight: 700;
        font-style: normal;
    }
    
    html, body, [class*="css"] {
        font-family: 'StabilGrotesk', sans-serif !important;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {visibility: hidden;}

    .stSidebar { min-width: 336px; }
    .stSidebar .stHeading { color: #FAFAFA; }
    .stSidebar .stElementContainer { width: auto; }
    .stAppHeader { display: none; }

    .stMainBlockContainer div[data-testid="stVerticalBlock"] > div[data-testid="stElementContainer"] > div[data-testid="stButton"] { text-align: center; }
    .stMainBlockContainer div[data-testid="stVerticalBlock"] > div[data-testid="stElementContainer"] > div[data-testid="stButton"] button {
        color: #FAFAFA;
        border: 1px solid #FAFAFA33;
        transition: all 0.3s ease;
        background-color: #0E1117;
        width: fit-content;
    }
    .stMainBlockContainer div[data-testid="stVerticalBlock"] > div[data-testid="stElementContainer"] > div[data-testid="stButton"] button:hover {
        transform: translateY(-2px);
    }

    /* Multi-line button support for sidebar */
    .stSidebar button {
        white-space: normal !important;
        word-wrap: break-word !important;
        height: auto !important;
        min-height: 2.5rem !important;
        padding: 0.5rem 1rem !important;
        text-align: left !important;
    }

    /* Metric cards */
    .metric-card {
        text-align: center;
        padding: 20px;
        border: 1px solid #333;
        border-radius: 8px;
        background-color: #0E1117;
    }
    .metric-value {
        font-size: 36px;
        font-weight: 700;
        color: #FAFAFA;
    }
    .metric-label {
        font-size: 14px;
        color: #888;
        margin-bottom: 8px;
    }
    
    /* Health score colors */
    .health-excellent { color: #4CAF50; }
    .health-good { color: #8BC34A; }
    .health-warning { color: #FFA726; }
    .health-critical { color: #F44336; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------
# Initialize Session State
# ------------------------------

if "initialized" not in st.session_state:
    st.session_state.initialized = False
    st.session_state.bq_connector = None
    st.session_state.report_generator = None
    st.session_state.rag_system = None
    st.session_state.chat_history = []
    st.session_state.question_history = []
    st.session_state.current_report = None

# ------------------------------
# Sidebar - Configuration & Navigation
# ------------------------------

with st.sidebar:
    st.image("https://www.dentsu.com/assets/images/main-logo-alt.png", width=160)
    
    # Start New Audit button
    if st.button("🧹 Start New Audit", use_container_width=True):
        st.session_state.chat_history = []
        st.session_state.question_history = []
        st.session_state.current_report = None
        if st.session_state.bq_connector:
            st.session_state.bq_connector.clear_all_cache()
        st.rerun()
    
    st.header("BigQuery Audit Dashboard")
    
    st.markdown(
        """
    **About the Tool**
    
    🔍 Instant data quality insights from your GA4 BigQuery data
    
    📊 Detect spikes, anomalies, drop-offs automatically
    
    🎯 AI-powered recommendations for fixing issues
    
    💡 Learn from past audits with RAG system
    """
    )
    
    st.divider()
    
    # Configuration
    st.subheader("⚙️ Configuration")
    
    # Get config from Streamlit secrets or environment
    project_id = st.text_input(
        "GCP Project ID",
        value=get_config_value("GCP_PROJECT_ID", "gcp-wow-food-wownz-wdl-prod"),
        help="Your Google Cloud Project ID"
    )
    
    dataset_id = st.text_input(
        "Dataset ID",
        value=get_config_value("GCP_DATASET_ID", "svfc"),
        help="BigQuery dataset name"
    )
    
    table_name = st.text_input(
        "Table Name",
        value=get_config_value("GCP_TABLE_NAME", "ga_events"),
        help="BigQuery table name"
    )
    
    # Show credential status
    credentials_path = get_credentials_path()
    if credentials_path:
        st.success("✅ Credentials found")
    else:
        st.warning("⚠️ No credentials configured")
        st.info("Add GCP_CREDENTIALS to Streamlit secrets or .env file")
    
    # Initialize system
    if st.button("🚀 Initialize System", use_container_width=True, type="primary"):
        if not credentials_path:
            st.error("❌ Cannot initialize: No GCP credentials found. Please add credentials to Streamlit secrets or .env file.")
        else:
            try:
                with st.spinner("Initializing BigQuery connection..."):
                    # Initialize BigQuery connector
                    st.session_state.bq_connector = WoolworthsBigQueryConnector(
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_name=table_name,
                        credentials_path=credentials_path
                    )
                    
                    # Initialize report generator
                    st.session_state.report_generator = WoolworthsReportGenerator(
                        st.session_state.bq_connector
                    )
                    
                    # Initialize RAG system
                    st.session_state.rag_system = SimpleRAGSystem()
                    
                    # Initialize chat history with system prompt
                    audit_context = f"""
You are auditing BigQuery data for Woolworths NZ.
Project: {project_id}
Dataset: {dataset_id}
Table: {table_name}

You help GA4 analysts understand data quality issues and provide actionable recommendations.
Be concise, specific, and always cite actual data when available.
"""
                    st.session_state.chat_history = [
                        {"role": "system", "content": audit_context}
                    ]
                    
                    st.session_state.initialized = True
                    st.success("✅ System initialized successfully!")
                    st.rerun()
            
            except Exception as e:
                st.error(f"❌ Initialization failed: {str(e)}")
                st.info("Check your credentials and permissions")
    
    st.divider()
    
    # How to Use
    st.markdown(
        """
    **How to Use**
    
    1️⃣ **Configure** - Check your GCP project details above
    
    2️⃣ **Initialize** - Click 'Initialize System' button
    
    3️⃣ **Generate Report** - Click 'Generate Full Audit Report'
    
    4️⃣ **Review Metrics** - Check health score and critical issues
    
    5️⃣ **Expand Sections** - Dive into specific audit areas
    
    6️⃣ **Ask Questions** - Use chat to explore findings
    
    7️⃣ **Export** - Download report as Markdown or CSV
    """
    )
    
    st.divider()
    
    # Recent Questions
    st.subheader("📋 Recent Questions")
    if st.session_state.question_history:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        today_qs = [q for q in st.session_state.question_history if q["date"] == today]
        yesterday_qs = [q for q in st.session_state.question_history if q["date"] == yesterday]
        
        if today_qs:
            st.markdown("**Today**")
            for q in reversed(today_qs[-5:]):
                if st.button(q["text"], key=f"today_{q['timestamp']}", use_container_width=True):
                    st.session_state.rerun_question = q["text"]
                    st.rerun()
        
        if yesterday_qs:
            st.markdown("**Yesterday**")
            for q in reversed(yesterday_qs[-5:]):
                if st.button(q["text"], key=f"yesterday_{q['timestamp']}", use_container_width=True):
                    st.session_state.rerun_question = q["text"]
                    st.rerun()
    else:
        st.info("No questions yet. Start asking!")
    
    st.divider()
    
    # Cache stats (if initialized)
    if st.session_state.initialized and st.session_state.bq_connector:
        cache_stats = st.session_state.bq_connector.get_cache_stats()
        st.markdown("**⚡ Cache Stats**")
        st.markdown(f"Memory: {cache_stats['memory_cache_items']} items")
        st.markdown(f"Disk: {cache_stats['disk_cache_items']} items")
        
        if st.button("🗑️ Clear Cache", use_container_width=True):
            st.session_state.bq_connector.clear_all_cache()
            st.success("Cache cleared!")
            st.rerun()

# ------------------------------
# Main Content Area
# ------------------------------

# Check if system is initialized
if not st.session_state.initialized:
    st.title("🔍 Woolworths NZ BigQuery Audit Dashboard")
    st.markdown("**Project:** Woolworths NZ GA4 Data Quality")
    
    st.info("👈 Please configure and initialize the system using the sidebar to begin.")
    
    st.markdown("""
    ### What This Tool Does
    
    This dashboard automatically audits your Woolworths NZ GA4 BigQuery data to detect:
    
    - 📈 **Traffic Spikes** - Unusual increases in event volume
    - 📉 **Event Drop-offs** - Events that stopped firing or decreased significantly
    - 🔍 **Null Rate Issues** - Missing critical fields in your data
    - 💰 **Ecommerce Problems** - Missing revenue or transaction data
    - 🏪 **Store Tracking** - Store name and loyalty ID capture rates
    - 🎯 **Promotion Tracking** - Promotion field completeness
    - 🥗 **Recipe Content** - Recipe-related field population
    - ⚡ **Data Freshness** - How up-to-date your data is
    - 🔗 **Traffic Source Health** - Source/medium/campaign tracking
    
    ### Features
    
    - ✅ **3-Tier Caching** - Memory → Disk → BigQuery (reduces costs)
    - ✅ **Prebaked Reports** - One-click comprehensive audit
    - ✅ **AI Chat** - Ask questions about your data using GROQ
    - ✅ **RAG System** - Learns from past audits
    - ✅ **Export Options** - Download as Markdown or CSV
    
    ### Get Started
    
    1. Ensure you have credentials configured (see sidebar)
    2. Click "Initialize System"
    3. Generate your first audit report!
    """)
    
    st.stop()

# System is initialized - show main dashboard
st.title("🔍 BigQuery Audit Dashboard")
st.markdown(f"**Project:** `{st.session_state.bq_connector.full_table_id}`")

# ------------------------------
# Generate Prebaked Report Section
# ------------------------------

st.markdown("---")
st.subheader("📋 Prebaked Daily Audit Report")

col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("Generate a comprehensive audit report with all quality checks")
with col2:
    if st.button("🚀 Generate Full Audit Report", use_container_width=True, type="primary"):
        with st.spinner("Running comprehensive audit... (this may take 30-60 seconds)"):
            try:
                report = st.session_state.report_generator.generate_daily_audit_report()
                st.session_state.current_report = report
                st.success("✅ Audit report generated!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Report generation failed: {str(e)}")
                st.info("Check BigQuery permissions and table access")

# Display report if available
if st.session_state.current_report:
    report = st.session_state.current_report
    
    st.markdown("---")
    
    # Health Score Display (Prominent)
    health_score = report["health_score"]
    
    if health_score >= 90:
        score_color = "#4CAF50"
        score_status = "EXCELLENT"
        score_emoji = "🟢"
    elif health_score >= 75:
        score_color = "#8BC34A"
        score_status = "GOOD"
        score_emoji = "🟢"
    elif health_score >= 60:
        score_color = "#FFA726"
        score_status = "NEEDS ATTENTION"
        score_emoji = "🟡"
    else:
        score_color = "#F44336"
        score_status = "CRITICAL"
        score_emoji = "🔴"
    
    st.markdown(f"""
    <div style='text-align:center; padding:30px; border:2px solid {score_color}; 
         border-radius:12px; background-color:#0E1117; margin:20px 0;'>
        <div style='font-size:18px; color:#888; margin-bottom:12px;'>Data Health Score</div>
        <div style='font-size:72px; font-weight:700; color:{score_color};'>{health_score}</div>
        <div style='font-size:20px; color:{score_color}; margin-top:8px;'>{score_emoji} {score_status}</div>
        <div style='font-size:12px; color:#888; margin-top:8px;'>Generated: {report['generated_at'].strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Key Metrics Row
    st.markdown("### 📊 Key Metrics Overview")
    
    metric_cols = st.columns(4)
    
    with metric_cols[0]:
        critical_count = len(report["critical_issues"])
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-label'>Critical Issues</div>
            <div class='metric-value' style='color: {"#F44336" if critical_count > 0 else "#4CAF50"};'>{critical_count}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with metric_cols[1]:
        warning_count = sum(1 for section in report["sections"].values() if "🟡" in section["summary"])
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-label'>Warnings</div>
            <div class='metric-value' style='color: {"#FFA726" if warning_count > 0 else "#4CAF50"};'>{warning_count}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with metric_cols[2]:
        cache_hits = sum(1 for source in report["cache_performance"].values() if source != "query")
        total_queries = len(report["cache_performance"])
        cache_rate = int(cache_hits / total_queries * 100) if total_queries > 0 else 0
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-label'>Cache Hit Rate</div>
            <div class='metric-value' style='color: #4CAF50;'>{cache_rate}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    with metric_cols[3]:
        sections_checked = len(report["sections"])
        st.markdown(f"""
        <div class='metric-card'>
            <div class='metric-label'>Sections Audited</div>
            <div class='metric-value'>{sections_checked}</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Critical Issues Section (if any)
    if report["critical_issues"]:
        st.markdown("### 🔴 Critical Issues Requiring Immediate Attention")
        for idx, issue in enumerate(report["critical_issues"]):
            with st.expander(f"❗ {issue['category'].replace('_', ' ').title()} - {issue['severity']}", expanded=(idx == 0)):
                st.markdown(f"**Issue:** {issue['description']}")
                
                if issue['data_preview']:
                    st.markdown("**Affected Data (Preview):**")
                    preview_df = pd.DataFrame(issue['data_preview'])
                    st.dataframe(preview_df, use_container_width=True)
                
                # Ask RAG for insight
                if st.button(f"💡 Get AI Recommendation", key=f"rag_{idx}"):
                    with st.spinner("Generating recommendation..."):
                        try:
                            insight = st.session_state.rag_system.generate_insight({
                                "type": issue['category'],
                                "description": issue['description'],
                                "affected": str(issue['data_preview'][:2]) if issue['data_preview'] else "N/A"
                            })
                            st.markdown("**AI Recommendation:**")
                            st.info(insight)
                        except Exception as e:
                            st.error(f"Could not generate recommendation: {str(e)}")
    
    # Recommendations
    if report["recommendations"]:
        st.markdown("### 💡 Recommended Actions")
        for rec in report["recommendations"]:
            priority_color = "#F44336" if rec["priority"] == "CRITICAL" else ("#FF9800" if rec["priority"] == "HIGH" else "#FFC107")
            priority_emoji = "🔴" if rec["priority"] == "CRITICAL" else ("🟠" if rec["priority"] == "HIGH" else "🟡")
            
            st.markdown(f"""
            <div style='padding: 12px; border-left: 4px solid {priority_color}; background-color: #1E1E1E; border-radius: 4px; margin: 8px 0;'>
                <div style='font-weight: 700; color: {priority_color};'>{priority_emoji} [{rec["priority"]}] {rec["category"]}</div>
                <div style='margin-top: 8px;'>→ {rec["action"]}</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Detailed Section Results (Expandable)
    st.markdown("### 📊 Detailed Audit Results")
    
    for section_name, section_data in report["sections"].items():
        # Cache indicator
        cache_emoji = "💾" if section_data["cache_source"] == "disk" else "⚡" if section_data["cache_source"] == "memory" else "🔄"
        
        with st.expander(f"{cache_emoji} {section_name.replace('_', ' ').title()} - {section_data['summary']}"):
            if not section_data["data"].empty:
                st.dataframe(section_data["data"], use_container_width=True)
                
                # Download CSV option
                csv = section_data["data"].to_csv(index=False)
                st.download_button(
                    label=f"📥 Download {section_name} as CSV",
                    data=csv,
                    file_name=f"{section_name}_{report['report_date']}.csv",
                    mime="text/csv",
                    key=f"download_{section_name}"
                )
            else:
                st.info("No data issues detected in this section.")
    
    st.markdown("---")
    
    # Export Options
    st.markdown("### 📥 Export Full Report")
    
    export_col1, export_col2 = st.columns(2)
    
    with export_col1:
        # Markdown export
        if st.button("📄 Export as Markdown", use_container_width=True):
            with st.spinner("Generating Markdown report..."):
                try:
                    md_path = st.session_state.report_generator.export_report_to_markdown(
                        report,
                        output_path=f"audit_report_{report['report_date']}.md"
                    )
                    with open(md_path, 'r') as f:
                        md_content = f.read()
                    
                    st.download_button(
                        label="📄 Download Markdown Report",
                        data=md_content,
                        file_name=f"woolworths_audit_{report['report_date']}.md",
                        mime="text/markdown"
                    )
                except Exception as e:
                    st.error(f"Export failed: {str(e)}")
    
    with export_col2:
        # CSV export of critical issues
        if report["critical_issues"]:
            critical_df = pd.DataFrame(report["critical_issues"])
            critical_csv = critical_df.to_csv(index=False)
            
            st.download_button(
                label="📊 Download Critical Issues CSV",
                data=critical_csv,
                file_name=f"critical_issues_{report['report_date']}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.success("✅ No critical issues to export!")

# ------------------------------
# Quick Questions Section
# ------------------------------

st.markdown("---")
st.markdown("### 💡 Quick Questions")

quick_questions = [
    "📊 Show me event volume spikes from the last 2 weeks",
    "📉 Which events stopped firing or dropped significantly?",
    "🔍 What's the null rate for critical fields like purchase_revenue?",
    "💰 Are there any issues with ecommerce tracking?",
    "🏪 How well are we capturing store_name and loyalty_id?",
    "⚡ How fresh is our data? Any delays?",
    "🎯 Show me all critical issues detected",
    "📈 What's our overall data health score and why?",
]

# Display quick questions in 2 columns
q_col1, q_col2 = st.columns(2)

for idx, question in enumerate(quick_questions):
    target_col = q_col1 if idx % 2 == 0 else q_col2
    with target_col:
        if st.button(question, use_container_width=True, key=f"quick_q_{idx}"):
            st.session_state.rerun_question = question
            st.rerun()

st.markdown("---")

# ------------------------------
# Chat Interface
# ------------------------------

st.markdown("### 💬 Ask Questions About Your Data")

# Display chat history (skip system message)
for msg in st.session_state.chat_history:
    role = msg.get("role", "assistant")
    content = msg.get("content", "")
    
    if role == "system":
        continue  # Skip system prompt
    
    if role == "assistant":
        with st.chat_message("assistant"):
            st.markdown(content)
    else:
        with st.chat_message("user"):
            st.markdown(content)

# Handle preset question rerun
preset_input = None
if "rerun_question" in st.session_state:
    preset_input = st.session_state.rerun_question
    del st.session_state.rerun_question

# Chat input
user_input = st.chat_input("Ask me anything about your data quality...")
if preset_input:
    user_input = preset_input

if user_input:
    # Add to question history
    st.session_state.question_history.append({
        "text": user_input,
        "date": datetime.now().date(),
        "timestamp": datetime.now().isoformat()
    })
    
    # Add to chat history
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(user_input)
    
    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing your data..."):
            try:
                # Get GROQ API key from config
                groq_api_key = get_config_value("GROQ_API_KEY")
                
                if not groq_api_key:
                    st.error("❌ GROQ_API_KEY not found. Please add it to Streamlit secrets or .env file.")
                    st.stop()
                
                # Prepare audit context for the question
                audit_context = {}
                if st.session_state.current_report:
                    report = st.session_state.current_report
                    audit_context = {
                        "health_score": report["health_score"],
                        "critical_issues": len(report["critical_issues"]),
                        "warnings": sum(1 for s in report["sections"].values() if "🟡" in s["summary"]),
                        "issues": [
                            issue["description"] for issue in report["critical_issues"][:5]
                        ]
                    }
                
                # Get answer from RAG system
                response_text = st.session_state.rag_system.answer_question(
                    question=user_input,
                    audit_data=audit_context
                )
                
                # Display response
                st.markdown(response_text)
                
                # Add to chat history
                st.session_state.chat_history.append({
                    "role": "assistant",
                    "content": response_text
                })
            
            except Exception as e:
                error_msg = f"❌ Error generating response: {str(e)}"
                st.error(error_msg)
                st.info("Check that GROQ_API_KEY is configured correctly")
                st.session_state.chat_history.append({
                    "role": "assistant",
                    "content": error_msg
                })

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #888; font-size: 12px;'>
    Powered by Dentsu Analytics | Woolworths NZ GA4 Audit Dashboard v1.0
</div>
""", unsafe_allow_html=True)
