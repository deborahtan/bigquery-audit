# Woolworths NZ BigQuery Audit Dashboard

Automated GA4 data quality auditing for Woolworths/Fresh Choice analytics.

## 🎯 Features

- **Automated Audits**: Detect spikes, anomalies, drop-offs, and null rates automatically
- **3-Tier Caching**: Memory → Disk → BigQuery (reduces query costs by 80%+)
- **Prebaked Reports**: One-click comprehensive audit with health score
- **AI Chat**: Ask questions about your data using GROQ (Llama 3.1)
- **RAG System**: Learns from past audits to provide better recommendations
- **Export Options**: Download reports as Markdown or CSV

## 📊 What It Audits

### Traffic & Volume
- ✅ Daily spike detection (Z-score analysis)
- ✅ Event volume drop-offs (>30% decreases)
- ✅ Missing data gaps

### Data Quality
- ✅ Null rate per field per event per day
- ✅ Null rate trending over time
- ✅ Critical field validation (session_id, page_location, etc.)

### Ecommerce
- ✅ Purchase revenue completeness
- ✅ Item name/price population
- ✅ Cart value tracking

### Woolworths-Specific
- ✅ Store name capture rate
- ✅ Loyalty ID tracking
- ✅ Promotion tracking completeness
- ✅ Recipe/dietary field population
- ✅ Cuisine, course, ingredient tracking

### Operational
- ✅ Data freshness (latency detection)
- ✅ Traffic source health (source/medium/campaign)

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.9+
- GCP project with BigQuery access
- Service account with BigQuery permissions
- GROQ API key (get from https://console.groq.com/)

### 2. Installation
```bash
# Clone repository
git clone <your-repo-url>
cd bigquery_audit_dashboard

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env
```

Add these to your `.env`:
```bash
GCP_PROJECT_ID=gcp-wow-food-wownz-wdl-prod
GCP_DATASET_ID=svfc
GCP_TABLE_NAME=ga_events
GCP_CREDENTIALS_PATH=./credentials/service-account.json
GROQ_API_KEY=gsk_your_key_here
```

### 4. Add Fonts (Optional)

Place your StabilGrotesk fonts in:
```
static/StabilGrotesk-Regular.otf
static/StabilGrotesk-Bold.otf
```

### 5. Run
```bash
streamlit run app.py
```

Open browser to `http://localhost:8501`

## 📁 Project Structure
```
bigquery_audit_dashboard/
├── app.py                      # Main Streamlit app
├── .streamlit/
│   └── config.toml            # Dentsu styling
├── src/
│   ├── __init__.py
│   ├── bigquery_connector.py  # BQ queries (Woolworths schema)
│   ├── cache_manager.py       # 3-tier caching
│   ├── anomaly_detector.py    # Detection algorithms
│   ├── report_generator.py    # Prebaked reports
│   └── rag_system.py          # RAG with GROQ
├── static/                    # Fonts
├── cache/                     # Disk cache (auto-created)
├── .env.example              # Environment template
├── .gitignore                # Security
├── requirements.txt          # Dependencies
└── README.md                 # This file
```

## 🔧 Usage

### Generate Audit Report

1. Click **"Initialize System"** in sidebar
2. Wait for connection confirmation
3. Click **"Generate Full Audit Report"**
4. Review health score and critical issues
5. Expand sections for details
6. Export as Markdown or CSV

### Ask Questions

Use the chat interface:
- "Which events stopped firing yesterday?"
- "Show me null rates for purchase_revenue"
- "What's causing the spike on December 8th?"
- "How well are we tracking store names?"

### Quick Questions

Click preset questions for instant insights:
- Event volume spikes
- Missing events
- Null rate analysis
- Ecommerce validation
- Store/loyalty tracking

## 📊 Health Score

Health score is calculated from 0-100 based on:

- **Volume anomalies**: -10 points per high severity spike
- **Event drop-offs**: -15 points per missing event
- **Null rates**: -20 points per critical issue, -10 per warning
- **Ecommerce issues**: -15 points per critical revenue issue

**Score Ranges:**
- 90-100: 🟢 EXCELLENT
- 75-89: 🟢 GOOD
- 60-74: 🟡 NEEDS ATTENTION
- 0-59: 🔴 CRITICAL

## ⚡ Caching System

The 3-tier cache significantly reduces BigQuery costs:

1. **Memory Cache** (fastest): Streamlit session state
2. **Disk Cache** (medium): Pickle files in `./cache`
3. **BigQuery** (slowest): Only when cache misses

**Cache TTL:**
- Daily metrics: 6 hours
- Null rates: 12 hours
- Event drop-offs: 24 hours
- Freshness: 1 hour

Clear cache: Click "🗑️ Clear Cache" in sidebar

## 🔒 Security

### Files NEVER to commit:
```
.env                        # Real API keys
*.json                      # Service account files
credentials/               # All credential files
```

### Safe to commit:
```
.env.example               # Template only
.gitignore                 # Security config
app.py                     # Application code
```

## 🚀 Deployment

### Streamlit Community Cloud

1. Push to GitHub (secrets excluded via `.gitignore`)
2. Go to https://share.streamlit.io
3. Connect repository
4. Add secrets in dashboard:
```toml
GROQ_API_KEY = "gsk_your_key"
GCP_PROJECT_ID = "gcp-wow-food-wownz-wdl-prod"
GCP_DATASET_ID = "svfc"
GCP_TABLE_NAME = "ga_events"
GCP_CREDENTIALS = '''
{
  "type": "service_account",
  "project_id": "your-project",
  ...
}
'''
```

5. Deploy!

### Docker (Self-Hosted)
```bash
# Build
docker build -t bq-audit-dashboard .

# Run
docker run -p 8501:8501 --env-file .env bq-audit-dashboard
```

## 🐛 Troubleshooting

### "GROQ_API_KEY not found"

Add to `.env`:
```bash
GROQ_API_KEY=gsk_your_key_here
```

### "Permission denied" (BigQuery)

Your service account needs:
- `bigquery.jobs.create`
- `bigquery.jobs.get`
- `bigquery.tables.getData`

### "Cache directory not found"

The `./cache` directory is auto-created. Check permissions.

### Font not loading

Ensure fonts are in `./static/` directory. The app works without fonts (falls back to system fonts).

## 📝 Customization

### Change Cache TTL

Edit `src/cache_manager.py`:
```python
self.ttl_config = {
    "daily_spikes": 6,      # Change to your preference
    "null_rates": 12,
    ...
}
```

### Add New Audit Check

1. Add query to `src/bigquery_connector.py`
2. Add summary method to `src/report_generator.py`
3. Report will auto-include it

### Modify Health Score

Edit `src/report_generator.py`:
```python
def _calculate_health_score(self, report: Dict) -> int:
    score = 100
    # Adjust deduction logic here
    ...
```

## 🤝 Support

For issues or questions:
- Check troubleshooting section above
- Review logs in terminal
- Check BigQuery permissions
- Verify GROQ API key is valid

## 📄 License

Internal use - Dentsu
---

**Built by Dentsu Team**
