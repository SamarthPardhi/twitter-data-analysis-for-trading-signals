"""
analyze.py: Analyzes processed tweets and generates trading signals.

This script is updated to:
- Load data from the Parquet file created by process.py.
- Generate sentiment scores from the cleaned text.
- Create a composite signal, acknowledging that engagement data may be synthetic.
- Aggregate signals over time, noting timestamps may reflect scrape time, not post time.
- Create memory-efficient visualizations.
"""
import pandas as pd
import numpy as np
from textblob import TextBlob
import matplotlib.pyplot as plt
import logging
from pathlib import Path

# --- Configuration ---
PROCESSED_DATA_FILE = Path('processed_tweets.parquet')
PLOT_OUTPUT_FILE = Path('market_signal_plot.png')
AGGREGATION_WINDOW = '15T' # Aggregate signals into 15-minute windows

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_sentiment_signal(df: pd.DataFrame) -> pd.DataFrame:
    """Generates a sentiment polarity score (-1 to 1) for each tweet."""
    logging.info("Generating sentiment signals...")
    # TextBlob is robust enough for this task. For production, a fine-tuned model would be better.
    df['sentiment'] = df['cleaned_content'].apply(lambda x: TextBlob(x).sentiment.polarity)
    return df

def aggregate_signals(df: pd.DataFrame, window: str) -> pd.DataFrame:
    """Aggregates signals into time-based windows with confidence intervals."""
    logging.info(f"Aggregating signals into {window} windows...")
    if df.empty or 'timestamp_utc' not in df.columns:
        logging.warning("DataFrame is empty or missing timestamp. Skipping aggregation.")
        return pd.DataFrame()
        
    df.set_index('timestamp_utc', inplace=True)
    
    # Create a composite signal. Sentiment is weighted higher as it's more reliable than
    # the potentially random engagement data from the scraper.
    df['engagement'] = np.log1p(df['likes'] + df['retweets'] * 1.5 + df['replies'])
    # Normalize engagement to be roughly in the same scale as sentiment [-1, 1]
    df['engagement_normalized'] = (df['engagement'] - df['engagement'].mean()) / df['engagement'].std()
    
    df['composite_signal'] = (df['sentiment'] * 0.7) + (df['engagement_normalized'] * 0.3)

    # Resample and aggregate to get statistics for each time window
    aggregated = df['composite_signal'].resample(window).agg(['mean', 'std', 'count'])
    aggregated['std'].fillna(0, inplace=True)
    
    # Calculate 95% Confidence Interval for the mean signal
    z_score = 1.96
    aggregated['ci_lower'] = aggregated['mean'] - z_score * (aggregated['std'] / np.sqrt(aggregated['count']))
    aggregated['ci_upper'] = aggregated['mean'] + z_score * (aggregated['std'] / np.sqrt(aggregated['count']))
    
    return aggregated.dropna()

def plot_aggregated_signals(aggregated_df: pd.DataFrame):
    """Creates and saves a memory-efficient plot of the aggregated signals."""
    if aggregated_df.empty:
        logging.warning("No data to plot.")
        return
        
    logging.info("Generating and saving signal plot...")
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, ax = plt.subplots(figsize=(15, 7))
    
    aggregated_df['mean'].plot(ax=ax, label='Aggregated Market Signal (Mean)', color='royalblue', lw=2)
    ax.fill_between(aggregated_df.index,
                    aggregated_df['ci_lower'],
                    aggregated_df['ci_upper'],
                    color='skyblue', alpha=0.4, label='95% Confidence Interval')
    
    ax.set_title('Aggregated Market Sentiment Signal Over Time', fontsize=16)
    ax.set_xlabel('Timestamp (UTC)', fontsize=12)
    ax.set_ylabel('Composite Signal Value', fontsize=12)
    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    plt.tight_layout()
    plt.savefig(PLOT_OUTPUT_FILE)
    logging.info(f"Signal plot saved to {PLOT_OUTPUT_FILE}")

def main():
    """Main analysis pipeline."""
    logging.info("Starting data analysis script...")
    
    # --- IMPORTANT CAVEAT ---
    logging.warning("Analysis is based on data from scrapper.py, which may contain RANDOMIZED engagement and timestamp data if real-time parsing fails. Interpret results with caution.")
    
    if not PROCESSED_DATA_FILE.is_file():
        logging.error(f"Processed data file not found at {PROCESSED_DATA_FILE}. Please run process.py first.")
        return
        
    df = pd.read_parquet(PROCESSED_DATA_FILE)
    
    # 1. Generate sentiment signals
    df = generate_sentiment_signal(df)

    # 2. Aggregate signals into a time-series
    agg_signals = aggregate_signals(df, window=AGGREGATION_WINDOW)
    
    if not agg_signals.empty:
        logging.info("\n--- Sample of Aggregated Signals ---")
        print(agg_signals.head().to_string())
        logging.info("------------------------------------\n")
    
        # 3. Memory-efficient visualization
        plot_aggregated_signals(agg_signals)
    else:
        logging.warning("No signals were generated after aggregation.")

if __name__ == "__main__":
    # Ensure TextBlob corpora are downloaded (only needs to be done once)
    # import nltk; nltk.download('punkt'); nltk.download('brown')
    main()