import os
import datetime
import socket
import re
from typing import List, Dict, Any
import requests
import feedparser  
from googleapiclient.discovery import build  
from googleapiclient.errors import HttpError  
from dotenv import load_dotenv

socket.setdefaulttimeout(5)

load_dotenv()

def load_api_keys() -> None:
    """Load environment variables."""
    load_dotenv()

def _is_relevant(text: str) -> bool:
    if not text:
        return False

    text = text.lower()

    patterns = [
        r"\bleapscholar\b",
        r"\bleap scholar\b",
        r"\bleap finance\b",
        r"\bleap ielts\b",
    ]

    return any(re.search(pattern, text) for pattern in patterns)

def harvest_youtube() -> List[Dict[str, Any]]:
    """
    Search YouTube for strict brand terms.
    """
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        return []

    results = []
    
    query = "LeapScholar review|Leap Scholar IELTS|Leap Finance review"
    
    try:
        youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
        
        search_response = youtube.search().list(
            q=query,
            part="id,snippet",
            maxResults=10, 
            type="video",
            order="date"
        ).execute()

        for search_result in search_response.get("items", []):
            try:
                video_id = search_result["id"]["videoId"]
                snippet = search_result["snippet"]
                title = snippet.get("title", "")
                description = snippet.get("description", "")
                published_at = snippet.get("publishedAt", "")
                video_url = f"https://www.youtube.com/watch?v={video_id}"

                # Fetch comments
                comments_text = ""
                try:
                    comment_response = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=2,
                        textFormat="plainText"
                    ).execute()
                    
                    comments_list = []
                    for item in comment_response.get("items", []):
                        try:
                            comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                            comments_list.append(comment)
                        except Exception:
                            continue
                            
                    if comments_list:
                        comments_text = " | ".join(comments_list)
                except Exception:
                    pass

                full_text = f"{description}\nComments: {comments_text}"
                
                # Strict Filter
                if not (_is_relevant(title) or _is_relevant(full_text)):
                    continue

                results.append({
                    "source": "youtube",
                    "title": title,
                    "text": full_text,
                    "url": video_url,
                    "timestamp": published_at,
                    "keyword": "LeapScholar",
                    "matched": True
                })

            except Exception:
                continue

    except Exception:
        return []

    return results

def harvest_reddit() -> List[Dict[str, Any]]:
    """
    Search Reddit site-wide via JSON endpoint.
    """
    results = []
    # Site-wide search with specific OR query, sorted by new, limit 25
    url = 'https://www.reddit.com/search.json?q="Leap Scholar"+OR+"LeapScholar"+OR+"Leap Finance"&sort=new&limit=25'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
    }

    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code != 200:
            return []
            
        data = resp.json()
        children = data.get("data", {}).get("children", [])
        
        for post in children:
            try:
                post_data = post.get("data", {})
                title = post_data.get("title", "")
                selftext = post_data.get("selftext", "")
                permalink = post_data.get("permalink", "")
                created_utc = post_data.get("created_utc", 0)
                
                post_url = f"https://www.reddit.com{permalink}"
                timestamp = datetime.datetime.fromtimestamp(created_utc).isoformat()
                
                full_text = selftext
                
                # Strict Filter: check if title or text contains brand
                if not (_is_relevant(title) or _is_relevant(full_text)):
                    continue

                results.append({
                    "source": "reddit",
                    "title": title,
                    "text": full_text,
                    "url": post_url,
                    "timestamp": timestamp,
                    "keyword": "LeapScholar",
                    "matched": True
                })
            except Exception:
                continue
                
    except Exception:
        return []

    return results

def harvest_google_news() -> List[Dict[str, Any]]:
    """
    Fetch Google News RSS for brand term.
    """
    results = []
    feed_url = "https://news.google.com/rss/search?q=LeapScholar&hl=en-IN&gl=IN&ceid=IN:en"

    try:
        try:
            resp = requests.get(feed_url, timeout=5)
            if resp.status_code != 200:
                return []
            content = resp.content
        except Exception:
            return []

        feed = feedparser.parse(content)
        if not feed.entries:
            return []

        for entry in feed.entries[:10]:
            title = entry.get("title", "")
            summary = entry.get("summary", "")
            if not summary and hasattr(entry, "description"):
                summary = entry.description
            
            link = entry.get("link", "")
            
            timestamp = ""
            if hasattr(entry, "published"):
                 timestamp = entry.published
            elif hasattr(entry, "updated"):
                 timestamp = entry.updated
            
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    dt = datetime.datetime(*entry.published_parsed[:6])
                    timestamp = dt.isoformat()
                except Exception:
                    pass

            full_text = summary
            
            if not (_is_relevant(title) or _is_relevant(full_text)):
                continue

            results.append({
                "source": "google_news",
                "title": title,
                "text": full_text,
                "url": link,
                "timestamp": timestamp,
                "keyword": "LeapScholar",
                "matched": True
            })

    except Exception:
        return []

    return results

def harvest_all() -> List[Dict[str, Any]]:
    """
    Aggregates results from strict brand sources only.
    """
    load_api_keys()
    
    all_data = []

   
    try:
        yt_data = harvest_youtube()
        all_data.extend(yt_data)
    except Exception:
        pass
            
    try:
        reddit_data = harvest_reddit()
        all_data.extend(reddit_data)
    except Exception:
        pass
        

    try:
        news_data = harvest_google_news()
        all_data.extend(news_data)
    except Exception:
        pass
            
    return all_data
