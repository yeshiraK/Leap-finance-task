import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from harvesters import harvest_all
from engine import process_data


st.set_page_config(
    page_title="LeapPulse — Brand Perception Monitor",
    page_icon="📊",
    layout="wide"
)

def main():

    st.title("LeapPulse — Brand Perception Monitor")
    st.markdown("Real-time intelligence on brand sentiment and trending topics.")


    st.sidebar.header("Control Panel")
    if st.sidebar.button("Refresh Data"):
        st.experimental_rerun()
    
    st.sidebar.markdown(f"**Last Updated:** {datetime.now().strftime('%H:%M:%S')}")
    
    st.sidebar.selectbox(
        "Time Window",
        ["Last 24 Hours", "Last 7 Days", "Last 30 Days"]
    )


    with st.spinner("Harvesting data from sources..."):
        try:
            raw_data = harvest_all()
        except Exception as e:
            st.error(f"Failed to harvest data: {str(e)}")
            return

    if not raw_data:
        st.warning("No data found. Please check your API keys or try again later.")
        return

    with st.spinner("Processing intelligence..."):
        try:
            analytics = process_data(raw_data)
        except Exception as e:
            st.error(f"Failed to process data: {str(e)}")
            return


    records = analytics.get("cleaned_records", [])
    sentiment_summary = analytics.get("sentiment_summary", {})
    trends = analytics.get("trends", [])

    if not records:
        st.info("No records to display.")
        return

    df = pd.DataFrame(records)

    st.markdown("### The Pulse")
    
    col1, col2, col3 = st.columns(3)

    total_mentions = len(df)
    
    positive_count = sentiment_summary.get("positive", 0)
    neutral_count = sentiment_summary.get("neutral", 0)
    negative_count = sentiment_summary.get("negative", 0)

    if positive_count >= max(neutral_count, negative_count):
        majority_sentiment = "Positive"
        sentiment_delta = "↗"
    elif negative_count >= max(positive_count, neutral_count):
        majority_sentiment = "Negative"
        sentiment_delta = "↘"
    else:
        majority_sentiment = "Neutral"
        sentiment_delta = "➡"

    if total_mentions > 0:
        positive_ratio = (positive_count / total_mentions) * 100
    else:
        positive_ratio = 0.0

    with col1:
        st.metric(label="Overall Sentiment", value=majority_sentiment, delta=sentiment_delta)
    
    with col2:
        st.metric(label="Total Mentions", value=total_mentions)
        
    with col3:
        st.metric(label="Positive Ratio", value=f"{positive_ratio:.1f}%")

    st.markdown("---")


    col_trends, col_sources = st.columns([2, 1])

    with col_trends:
        st.subheader("Trending Conversations")
        if trends:
            trend_df = pd.DataFrame(trends, columns=["keyword", "score"])
            
            trend_df = trend_df.sort_values(by="score", ascending=False).head(10)
            
            if not trend_df.empty:
                chart = alt.Chart(trend_df).mark_bar().encode(
                    x=alt.X('score', title='Relevance Score'),
                    y=alt.Y('keyword', sort='-x', title='Keyword'),
                    color=alt.Color('score', scale=alt.Scale(scheme='blues'), legend=None),
                    tooltip=['keyword', 'score']
                ).properties(height=300)
                
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No sufficient trend data available.")
        else:
            st.info("No trends detected.")

    with col_sources:
        st.subheader("Source Breakdown")
        if "source" in df.columns:
            source_counts = df["source"].value_counts().reset_index()
            source_counts.columns = ["source", "count"]
            
            if not source_counts.empty:
                pie = alt.Chart(source_counts).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta(field="count", type="quantitative"),
                    color=alt.Color(field="source", type="nominal"),
                    tooltip=["source", "count"]
                ).properties(height=300)
                
                st.altair_chart(pie, use_container_width=True)
            else:
                st.info("No source data available.")
        else:
            st.info("Source information missing.")

    st.markdown("---")

    st.subheader("⚠ Critical Alerts")
    
    if "sentiment" in df.columns:
        negative_df = df[df["sentiment"] == "negative"].copy()
        
        if not negative_df.empty:
            if "timestamp" in negative_df.columns:
                try:
                    negative_df["timestamp"] = pd.to_datetime(negative_df["timestamp"], errors='coerce')
                    negative_df = negative_df.sort_values(by="timestamp", ascending=False)
                except Exception:
                    pass

            display_cols = ["source", "title", "timestamp", "url"]
            display_cols = [c for c in display_cols if c in negative_df.columns]
            
            st.dataframe(
                negative_df[display_cols].head(5),
                use_container_width=True
            )
        else:
            st.success("No critical alerts detected in this batch.")
    else:
        st.info("Sentiment data not available.")


    with st.expander("View Full Data Log"):
        st.dataframe(df)

if __name__ == "__main__":
    main()
