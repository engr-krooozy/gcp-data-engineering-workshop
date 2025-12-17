import streamlit as st
from google.cloud import bigquery
import os
import pandas as pd
import altair as alt

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("BQ_DATASET")
TABLE_ID = os.environ.get("BQ_TABLE")

st.set_page_config(page_title="Stock Analysis Dashboard", layout="wide")

st.title("📈 Real-Time Stock Analysis Dashboard")

if not PROJECT_ID or not DATASET_ID or not TABLE_ID:
    st.error("Missing environment variables. Please check your deployment.")
    st.info(f"debug info: PROJECT_ID={PROJECT_ID}, DATASET_ID={DATASET_ID}, TABLE_ID={TABLE_ID}")
else:
    st.markdown(f"Displaying results from BigQuery table: `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`")
    try:
        client = bigquery.Client(project=PROJECT_ID)
        
        # --- Market Overview Section ---
        st.header("🌍 Market Overview")
        
        # Query recent data for ALL tickers for the overview chart
        overview_query = f"""
            SELECT 
                window_timestamp,
                ticker,
                latest_price
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            ORDER BY window_timestamp DESC
            LIMIT 500
        """
        overview_job = client.query(overview_query)
        overview_df = overview_job.to_dataframe()
        
        if not overview_df.empty:
            # Altair Chart for All Stocks
            base = alt.Chart(overview_df).encode(
                x='window_timestamp:T',
                y=alt.Y('latest_price:Q', scale=alt.Scale(zero=False), title='Price ($)'),
                color='ticker:N',
                tooltip=['window_timestamp', 'ticker', 'latest_price']
            )
            line = base.mark_line()
            points = base.mark_circle(size=50) # Add points for easier tooltip hover
            
            chart = (line + points).properties(
                height=400,
                title="All Stocks - Price Movement"
            ).interactive()
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("No data available for market overview.")

        st.divider()

        # --- Individual Ticker Analysis ---
        st.header("🔍 Individual Analysis")
        
        # Get distinct tickers
        tickers = overview_df['ticker'].unique().tolist() if not overview_df.empty else []
        
        if not tickers:
             # Fallback if overview query failed or was empty but we want to try listing distinct tickers separately
            tickers_query = f"SELECT DISTINCT ticker FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
            tickers = [row.ticker for row in client.query(tickers_query).result()]

        if not tickers:
            st.warning("No tickers found.")
        else:
            selected_ticker = st.selectbox("Select Ticker", tickers)
            
            # Query data for selected ticker
            query = f"""
                SELECT 
                    window_timestamp,
                    latest_price,
                    high_price_1m,
                    total_volume_1m,
                    total_value_1m,
                    sma_5m,
                    is_volume_spike,
                    system_latency,
                    ai_sentiment,
                    ai_summary
                FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                WHERE ticker = '{selected_ticker}'
                ORDER BY window_timestamp DESC
                LIMIT 100
            """
            
            query_job = client.query(query)
            df = query_job.to_dataframe()
            
            if df.empty:
                st.warning("No data rows found for this ticker.")
            else:
                # Latest metrics
                latest = df.iloc[0]
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Latest Price", f"${latest['latest_price']:.2f}")
                with col2:
                    st.metric("Volume (1m)", f"{latest['total_volume_1m']:,}")
                with col3:
                    st.metric("SMA (5m)", f"${latest['sma_5m']:.2f}" if pd.notnull(latest['sma_5m']) else "N/A")
                with col4:
                    sentiment = latest['ai_sentiment']
                    color = "normal"
                    st.metric("AI Sentiment", f"{sentiment:.2f}")

                # Detailed Chart with SMA
                st.subheader(f"{selected_ticker} Price Analysis")
                
                base = alt.Chart(df).encode(x='window_timestamp:T')
                
                # Price Line and Points
                line_price = base.mark_line(color='blue').encode(
                    y=alt.Y('latest_price:Q', scale=alt.Scale(zero=False), title='Price ($)')
                )
                points_price = base.mark_circle(color='blue', size=50).encode(
                    y='latest_price:Q',
                    tooltip=['window_timestamp', 'latest_price']
                )
                
                # SMA Line (dashed) and Points
                line_sma = base.mark_line(color='orange', strokeDash=[5, 5]).encode(
                   y=alt.Y('sma_5m:Q', scale=alt.Scale(zero=False))
                )
                points_sma = base.mark_circle(color='orange', size=30).encode(
                   y='sma_5m:Q',
                   tooltip=['window_timestamp', 'sma_5m']
                )
                
                c = (line_price + points_price + line_sma + points_sma).properties(height=400).interactive()
                st.altair_chart(c, use_container_width=True)

                # AI Insights Section
                st.subheader("🤖 AI Insights")
                if latest['ai_summary'] and latest['ai_summary'] != 'No summary':
                    st.info(latest['ai_summary'])
                else:
                    st.text("No AI summary available for the latest window.")
                
                # Raw Data
                with st.expander("View Raw Data"):
                    st.dataframe(df)

    except Exception as e:
        st.error(f"Error connecting to BigQuery: {e}")
