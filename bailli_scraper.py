import requests
from bs4 import BeautifulSoup
import json
import time
from tqdm import tqdm
import re
import textwrap
import os
from pathlib import Path
from typing import List, Dict, Optional
import logging
from anthropic import Anthropic

BASE_URL = "https://www.bailii.org"
UK_COURTS_INDEX = f"{BASE_URL}/uk/cases/"
CHUNK_CHAR_LIMIT = 4000  # Adjust for token limits (4k characters ~ 1000 tokens)

# Initialize Anthropic client
client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

logger = logging.getLogger(__name__)

class BailliScraper:
    def __init__(self, output_dir: str = "case_law"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract_case_content(self, html_content: str, url: str) -> Optional[Dict]:
        """Extract case content from HTML"""
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            text = soup.get_text(separator="\n")
            lines = text.splitlines()
            
            # Basic case information
            case_data = {
                'url': url,
                'title': lines[0].strip() if lines else "Unknown Case",
                'content': text.strip(),
                'summary': ' '.join(lines[1:5]) if len(lines) > 1 else ""
            }
            
            return case_data
            
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
            return None

uk_courts = [
    "UKSC", "EWCA/Civ", "EWCA/Crim", "EWHC/Admin", "EWHC/Ch", "EWHC/Comm",
    "EWHC/Fam", "EWHC/QB", "EWHC/Tech", "UKUT", "UKFTT", "EWMC", "EWCC"
]

def get_case_links_for_court(court_code):
    current_year = 2023
    index_url = f"{UK_COURTS_INDEX}{court_code}/{current_year}/"
    try:
        res = requests.get(index_url, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        links = [
            BASE_URL + a['href']
            for a in soup.find_all("a", href=True)
            if a['href'].endswith(".html") and court_code in a['href']
        ]
        return links
    except Exception as e:
        print(f"Failed to get links for {court_code}: {e}")
        return []

def chunk_text(text, max_len=CHUNK_CHAR_LIMIT):
    return textwrap.wrap(text, max_len, break_long_words=False, replace_whitespace=False)

def analyze_case_with_claude(case_text, case_name):
    """Use Claude to analyze legal case text and extract key information."""
    try:
        prompt = f"""Analyze this UK legal case and provide a structured analysis:

Case Text:
{case_text}

Please provide:
1. Key legal issues and reasoning
2. Referenced legislation and legal precedents
3. Court's decision and rationale
4. Legal principles established or applied

Format your response as structured text suitable for fine-tuning data."""

        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1000,
            temperature=0.1,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return message.content[0].text
    except Exception as e:
        print(f"Error analyzing case with Claude: {e}")
        return f"Analysis unavailable for {case_name}"

def extract_case_data_with_chunks(url):
    try:
        res = requests.get(url, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        text = soup.get_text(separator="\n")
        lines = text.splitlines()
        case_name = lines[0].strip() if lines else "Unknown Case"
        
        # Clean and prepare text for Claude analysis
        clean_text = re.sub(r'\s+', ' ', text).strip()
        
        # Use Claude to analyze the case
        claude_analysis = analyze_case_with_claude(clean_text[:8000], case_name)  # Limit input size
        
        # Create chunks for fine-tuning
        chunks = chunk_text(text)
        
        entries = []
        for i, chunk in enumerate(chunks):
            entries.append({
                "instruction": "Analyze this UK legal case text and provide detailed legal reasoning, referenced legislation, and key principles.",
                "input": chunk.strip(),
                "output": f"Case: {case_name} (Part {i+1})\n\nClaude Analysis:\n{claude_analysis}"
            })
        return entries
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return []

# Main scraping loop
all_cases = []

for court in uk_courts:
    print(f"Scraping court: {court}")
    links = get_case_links_for_court(court)
    for link in tqdm(links[:20], desc=f"Processing {court}"):  # Limit per court
        entries = extract_case_data_with_chunks(link)
        if entries:
            all_cases.extend(entries)
        time.sleep(1)  # Be polite

# Save the dataset
with open("uk_legal_cases_claude_analyzed.json", "w", encoding="utf-8") as f:
    json.dump(all_cases, f, indent=2)

print(f"✅ Saved {len(all_cases)} fine-tuning entries with Claude 4.0 Sonnet analysis.")
