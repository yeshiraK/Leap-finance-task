import re
import pandas as pd
from typing import List, Dict, Any, Tuple
from collections import Counter

# Global cache for models
VADER_ANALYZER = None
ROBERTA_PIPELINE = None
KEYWORD_MODEL = None
MODELS_LOADED = False

def _load_models():
    """
    Lazy load models safely.
    Defensive imports to prevent crashing on missing dependencies or version mismatch.
    """
    global VADER_ANALYZER, ROBERTA_PIPELINE, KEYWORD_MODEL, MODELS_LOADED
    
    if MODELS_LOADED:
        return

    # 1. Load VADER (Pure Python, usually safe)
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        VADER_ANALYZER = SentimentIntensityAnalyzer()
    except Exception:
        VADER_ANALYZER = None

    # 2. Load Transformers (RoBERTa)
    try:
        from transformers import pipeline
        # Use a specific, smaller model if possible, or catch failures
        ROBERTA_PIPELINE = pipeline(
            "sentiment-analysis", 
            model="cardiffnlp/twitter-roberta-base-sentiment",
            tokenizer="cardiffnlp/twitter-roberta-base-sentiment",
            max_length=512, 
            truncation=True
        )
    except Exception:
        # Fallback to None if transformers/torch fails
        ROBERTA_PIPELINE = None

    # 3. Load KeyBERT
    try:
        from keybert import KeyBERT
        KEYWORD_MODEL = KeyBERT()
    except Exception:
        # Fallback to None if torch/numpy/keybert fails
        KEYWORD_MODEL = None
        
    MODELS_LOADED = True

def clean_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert raw records to a DataFrame and perform cleaning.
    """
    if not records:
        return pd.DataFrame(columns=["source", "title", "text", "url", "timestamp", "keyword", "content"])

    df = pd.DataFrame(records)

    def clean_text(text: Any) -> str:
        if not isinstance(text, str):
            return ""
        # Remove URLs
        text = re.sub(r'http\S+|www\.\S+', '', text, flags=re.MULTILINE)
        # Remove @mentions
        text = re.sub(r'@\w+', '', text)
        # Strip excessive whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    if 'title' in df.columns:
        df['title'] = df['title'].apply(clean_text)
    else:
        df['title'] = ""
        
    if 'text' in df.columns:
        df['text'] = df['text'].apply(clean_text)
    else:
        df['text'] = ""

    df['content'] = df['title'] + ". " + df['text']

    if 'url' in df.columns:
        df = df.drop_duplicates(subset=['url'])

    return df

def analyze_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze sentiment using VADER logic (robust fallback).
    """
    _load_models()
    
    if df.empty:
        df['sentiment'] = []
        return df

    sentiments = []
    
    roberta_map = {
        "LABEL_0": "negative",
        "LABEL_1": "neutral",
        "LABEL_2": "positive"
    }

    for idx, row in df.iterrows():
        source = row.get('source', '')
        content = row.get('content', '')
        
        if not str(content).strip():
            sentiments.append("neutral")
            continue

        sentiment_res = "neutral"

        try:
            # Prio 1: Transformers for detailed text (if available and not youtube)
            if source != 'youtube' and ROBERTA_PIPELINE:
                try:
                    # Truncate safely
                    safe_content = content[:1000] 
                    result = ROBERTA_PIPELINE(safe_content)
                    # Pipeline can return list of dicts or dict depending on version/args
                    if isinstance(result, list):
                        label = result[0]['label']
                    else:
                        label = result['label']
                    sentiment_res = roberta_map.get(label, "neutral")
                except Exception:
                    sentiment_res = _vader_sentiment(content)
            
            # Prio 2: VADER
            else:
                sentiment_res = _vader_sentiment(content)

        except Exception:
            sentiment_res = "neutral"
            
        sentiments.append(sentiment_res)

    df['sentiment'] = sentiments
    return df

def _vader_sentiment(text: str) -> str:
    """Helper for VADER scoring"""
    if not VADER_ANALYZER:
        return "neutral"
    
    try:
        scores = VADER_ANALYZER.polarity_scores(text)
        compound = scores.get('compound', 0)
        if compound >= 0.05:
            return 'positive'
        elif compound <= -0.05:
            return 'negative'
        else:
            return 'neutral'
    except Exception:
        return "neutral"

def extract_trends(df: pd.DataFrame) -> List[Tuple[str, float]]:
    """
    Extract trends using KeyBERT or simple frequency fallback.
    """
    _load_models()
    
    if df.empty:
        return []
    
    try:
        full_text = " ".join(df['content'].astype(str).tolist())
    except Exception:
        return []

    if not full_text.strip():
        return []

    # Method 1: KeyBERT
    if KEYWORD_MODEL:
        try:
            keywords = KEYWORD_MODEL.extract_keywords(
                full_text,
                keyphrase_ngram_range=(1, 2),
                stop_words='english',
                top_n=10
            )
            return keywords
        except Exception:
            pass # Fallback to simple extraction
    
    # Method 2: Simple Frequency Fallback
    return _simple_keyword_extraction(full_text)

def _simple_keyword_extraction(text: str) -> List[Tuple[str, float]]:
    """
    Fallback keyword extraction using word frequency.
    """
    try:
        stop_words = {
            'the', 'and', 'to', 'of', 'a', 'in', 'is', 'that', 'for', 'it', 'on', 
            'with', 'as', 'are', 'was', 'this', 'by', 'at', 'be', 'or', 'from',
            'an', 'not', 'you', 'we', 'have', 'can', 'has', 'but', 'if', 'leapscholar', 'leap', 'scholar',
            'video', 'comments', 'watch', 'http', 'https', 'com', 'www', 'review', 'about'
        }
        
        words = re.findall(r'\w+', text.lower())
        
        filtered_words = [
            w for w in words 
            if len(w) > 3 and w not in stop_words and not w.isdigit()
        ]
        
        counter = Counter(filtered_words)
        total = sum(counter.values()) if counter else 1
        
        return [(word, count/total) for word, count in counter.most_common(10)]
        
    except Exception:
        return []

def process_data(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Orchestrator for data processing.
    """
    response = {
        "cleaned_records": [],
        "sentiment_summary": {"positive": 0, "neutral": 0, "negative": 0},
        "trends": []
    }
    
    try:
        df_clean = clean_records(records)
        
        try:
            df_analyzed = analyze_sentiment(df_clean)
        except Exception:
            df_analyzed = df_clean
            df_analyzed['sentiment'] = "neutral"
            
        try:
            trends = extract_trends(df_analyzed)
        except Exception:
            trends = []
            
        if 'sentiment' in df_analyzed.columns:
            counts = df_analyzed['sentiment'].value_counts().to_dict()
        else:
            counts = {}
            
        response["sentiment_summary"] = {
            "positive": counts.get("positive", 0),
            "neutral": counts.get("neutral", 0),
            "negative": counts.get("negative", 0)
        }
        
        response["cleaned_records"] = df_analyzed.to_dict(orient="records")
        response["trends"] = trends
        
    except Exception:
        pass
        
    return response
