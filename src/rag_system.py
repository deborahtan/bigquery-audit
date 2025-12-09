"""
Simple RAG system for BigQuery audit insights using GROQ
"""

import os
from typing import List, Dict, Optional
from groq import Groq


class SimpleRAGSystem:
    """
    Lightweight RAG system that learns from past audits
    Uses GROQ for fast inference
    """
    
    def __init__(self):
        self.groq_api_key = os.getenv("GROQ_API_KEY")
        self.model = os.getenv("LLM_MODEL", "llama-3.1-8b-instant")
        
        if not self.groq_api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")
        
        self.client = Groq(api_key=self.groq_api_key)
        
        # Store past audit findings in memory (simple version)
        self.past_findings = []
    
    def add_audit_finding(self, finding: Dict):
        """
        Store an audit finding for future reference
        
        Args:
            finding: Dict with keys like 'date', 'issue_type', 'description', 'resolution'
        """
        self.past_findings.append(finding)
        
        # Keep only last 50 findings to avoid memory issues
        if len(self.past_findings) > 50:
            self.past_findings = self.past_findings[-50:]
    
    def search_similar_issues(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        Simple keyword-based search through past findings
        (In production, you'd use vector embeddings)
        """
        query_lower = query.lower()
        
        # Score findings by keyword overlap
        scored_findings = []
        for finding in self.past_findings:
            score = 0
            finding_text = f"{finding.get('issue_type', '')} {finding.get('description', '')}".lower()
            
            # Count matching words
            for word in query_lower.split():
                if len(word) > 3 and word in finding_text:
                    score += 1
            
            if score > 0:
                scored_findings.append((score, finding))
        
        # Sort by score and return top_k
        scored_findings.sort(reverse=True, key=lambda x: x[0])
        return [f[1] for f in scored_findings[:top_k]]
    
    def generate_insight(self, issue_data: Dict, context: str = "") -> str:
        """
        Generate AI-powered insight about a data quality issue
        
        Args:
            issue_data: Dict with issue details
            context: Additional context about the issue
        
        Returns:
            AI-generated insight and recommendation
        """
        # Search for similar past issues
        similar_issues = self.search_similar_issues(
            f"{issue_data.get('type', '')} {issue_data.get('description', '')}"
        )
        
        # Build context from similar issues
        past_context = ""
        if similar_issues:
            past_context = "\n\nSimilar issues from past audits:\n"
            for issue in similar_issues:
                past_context += f"- {issue.get('description', 'N/A')}: {issue.get('resolution', 'N/A')}\n"
        
        # Create prompt
        prompt = f"""You are a GA4 BigQuery data quality expert analyzing FreshChoice NZ data.

Current Issue:
- Type: {issue_data.get('type', 'Unknown')}
- Description: {issue_data.get('description', 'Unknown')}
- Affected: {issue_data.get('affected', 'Unknown')}
{context}
{past_context}

Provide:
1. Likely root cause (1-2 sentences)
2. Immediate action needed (specific steps)
3. How to prevent this in future

Be concise and actionable. Focus on practical solutions for a GA4 analyst."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a data quality expert specializing in GA4 BigQuery audits for retail analytics."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.7,
                max_tokens=500
            )
            
            return response.choices[0].message.content
        
        except Exception as e:
            return f"Could not generate insight: {str(e)}"
    
    def answer_question(self, question: str, audit_data: Dict) -> str:
        """
        Answer user questions about audit data using GROQ
        
        Args:
            question: User's question
            audit_data: Current audit data context
        
        Returns:
            AI-generated answer
        """
        # Build context from audit data
        context = f"""Current Audit Data Summary:
- Total events analyzed: {audit_data.get('total_events', 'N/A')}
- Date range: {audit_data.get('date_range', 'N/A')}
- Critical issues: {audit_data.get('critical_issues', 0)}
- Warnings: {audit_data.get('warnings', 0)}
- Health score: {audit_data.get('health_score', 'N/A')}/100
"""
        
        # Add any detected issues
        if audit_data.get('issues'):
            context += "\n\nDetected Issues:\n"
            for issue in audit_data.get('issues', [])[:5]:
                context += f"- {issue.get('description', 'N/A')}\n"
        
        prompt = f"""{context}

User Question: {question}

Answer the question based on the audit data above. Be specific and cite actual numbers from the data when possible. If you don't have enough information, say so."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a GA4 BigQuery audit assistant for FreshChoice NZ. Answer questions about data quality based on the provided audit data."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.7,
                max_tokens=800
            )
            
            return response.choices[0].message.content
        
        except Exception as e:
            return f"Could not generate answer: {str(e)}"


# Example usage
if __name__ == "__main__":
    # Initialize RAG system
    rag = SimpleRAGSystem()
    
    # Add a past finding
    rag.add_audit_finding({
        "date": "2024-12-01",
        "issue_type": "null_rate_spike",
        "description": "Session ID null rate jumped to 15%",
        "resolution": "GTM container had broken trigger - fixed by republishing container"
    })
    
    # Generate insight for new issue
    issue = {
        "type": "null_rate_spike",
        "description": "Purchase revenue field is null for 20% of purchase events",
        "affected": "purchase event"
    }
    
    insight = rag.generate_insight(issue)
    print(insight)
