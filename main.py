#!/usr/bin/env python3
"""
Fetch HYPE Historical Funding Rates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This script fetches all historical funding rates for HYPE from the earliest
available date to the current time and stores them in a CSV file.

Reference: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals#retrieve-historical-funding-rates
"""

import os
import sys
import json
import logging
import requests
import csv
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the parent directory to the path to import Utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hyperliquid_info_client import HyperliquidInfoClient


class HYPEHistoricalFundingRateFetcher:
    """
    Fetches historical funding rates for HYPE from Hyperliquid API.
    """
    
    def __init__(
        self,
        base_url: str = "https://api.hyperliquid.xyz",
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_delay: float = 0.1
    ):
        """
        Initialize the HYPE historical funding rate fetcher.
        
        Args:
            base_url: Hyperliquid API base URL
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            rate_limit_delay: Delay between requests to respect rate limits
        """
        self.client = HyperliquidInfoClient(
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            rate_limit_delay=rate_limit_delay
        )
        self.coin = "HYPE"
        
    def find_earliest_funding_data(self) -> int:
        """
        Find the earliest available funding data by testing different start dates.
        
        Returns:
            Timestamp in milliseconds of the earliest available funding data
        """
        logger.info("Searching for earliest available funding data...")
        
        # Test dates starting from a reasonable early date
        test_dates = [
            # Start from 2023 (when Hyperliquid launched)
            int(datetime(2023, 1, 1).timestamp() * 1000),
            # Try 2022
            int(datetime(2022, 1, 1).timestamp() * 1000),
            # Try 2021
            int(datetime(2021, 1, 1).timestamp() * 1000),
            # Try 2020
            int(datetime(2020, 1, 1).timestamp() * 1000),
        ]
        
        current_time = int(datetime.now().timestamp() * 1000)
        
        for start_time in test_dates:
            try:
                logger.info(f"Testing start date: {datetime.fromtimestamp(start_time/1000)}")
                
                # Fetch a small sample to see if data exists
                sample_data = self.client.get_funding_history(
                    coin=self.coin,
                    start_time=start_time,
                    end_time=min(start_time + (24 * 60 * 60 * 1000), current_time)  # 24 hours later
                )
                
                if sample_data and len(sample_data) > 0:
                    logger.info(f"Found earliest data starting from: {datetime.fromtimestamp(start_time/1000)}")
                    return start_time
                    
                time.sleep(self.client.rate_limit_delay)
                
            except Exception as e:
                logger.warning(f"Error testing date {datetime.fromtimestamp(start_time/1000)}: {e}")
                continue
        
        # If no data found in test dates, use a conservative default
        logger.warning("Could not determine earliest data date, using 2023-01-01 as default")
        return int(datetime(2023, 1, 1).timestamp() * 1000)
    
    def fetch_funding_rates_in_chunks(
        self,
        start_time: int,
        end_time: int,
        chunk_days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Fetch funding rates in chunks to avoid overwhelming the API.
        
        Args:
            start_time: Start time in milliseconds
            end_time: End time in milliseconds
            chunk_days: Number of days per chunk
            
        Returns:
            List of all funding rate entries
        """
        all_funding_rates = []
        chunk_ms = chunk_days * 24 * 60 * 60 * 1000  # Convert days to milliseconds
        
        current_start = start_time
        total_chunks = ((end_time - start_time) // chunk_ms) + 1
        current_chunk = 1
        
        logger.info(f"Fetching funding rates in {total_chunks} chunks of {chunk_days} days each")
        
        while current_start < end_time:
            current_end = min(current_start + chunk_ms, end_time)
            
            logger.info(f"Fetching chunk {current_chunk}/{total_chunks}: "
                       f"{datetime.fromtimestamp(current_start/1000)} to "
                       f"{datetime.fromtimestamp(current_end/1000)}")
            
            try:
                chunk_data = self.client.get_funding_history(
                    coin=self.coin,
                    start_time=current_start,
                    end_time=current_end
                )
                
                if chunk_data:
                    all_funding_rates.extend(chunk_data)
                    logger.info(f"Retrieved {len(chunk_data)} funding rate entries")
                else:
                    logger.info("No data found for this chunk")
                
                # Rate limiting
                time.sleep(self.client.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error fetching chunk {current_chunk}: {e}")
                # Continue with next chunk instead of failing completely
                
            current_start = current_end
            current_chunk += 1
        
        logger.info(f"Total funding rate entries retrieved: {len(all_funding_rates)}")
        return all_funding_rates
    
    def save_to_csv(self, funding_rates: List[Dict[str, Any]], filename: str = None) -> str:
        """
        Save funding rates to a CSV file.
        
        Args:
            funding_rates: List of funding rate dictionaries
            filename: Output filename (optional, will generate if not provided)
            
        Returns:
            Path to the created CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"hype_historical_funding_rates_{timestamp}.csv"
        
        # Ensure the filename has .csv extension
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        logger.info(f"Saving {len(funding_rates)} funding rate entries to {filename}")
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if funding_rates:
                # Get fieldnames from the first entry
                fieldnames = list(funding_rates[0].keys())
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for entry in funding_rates:
                    writer.writerow(entry)
            else:
                # Create empty CSV with expected headers
                fieldnames = ['coin', 'fundingRate', 'premium', 'time']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
        
        logger.info(f"Successfully saved funding rates to {filename}")
        return filename
    
    def fetch_and_save_historical_funding_rates(
        self,
        output_filename: str = None,
        chunk_days: int = 30,
        force_start_date: Optional[int] = None
    ) -> str:
        """
        Main method to fetch all historical funding rates and save to CSV.
        
        Args:
            output_filename: Output CSV filename (optional)
            chunk_days: Number of days per chunk for API requests
            force_start_date: Force a specific start date in milliseconds (optional)
            
        Returns:
            Path to the created CSV file
        """
        logger.info(f"Starting historical funding rate fetch for {self.coin}")
        
        # Determine start and end times
        if force_start_date:
            start_time = force_start_date
            logger.info(f"Using forced start date: {datetime.fromtimestamp(start_time/1000)}")
        else:
            start_time = self.find_earliest_funding_data()
        
        end_time = int(datetime.now().timestamp() * 1000)
        
        logger.info(f"Fetching funding rates from {datetime.fromtimestamp(start_time/1000)} "
                   f"to {datetime.fromtimestamp(end_time/1000)}")
        
        # Fetch all funding rates
        funding_rates = self.fetch_funding_rates_in_chunks(
            start_time=start_time,
            end_time=end_time,
            chunk_days=chunk_days
        )
        
        # Sort by timestamp to ensure chronological order
        funding_rates.sort(key=lambda x: x.get('time', 0))
        
        # Save to CSV
        csv_filename = self.save_to_csv(funding_rates, output_filename)
        
        # Print summary
        if funding_rates:
            first_entry = funding_rates[0]
            last_entry = funding_rates[-1]
            
            logger.info(f"\n=== SUMMARY ===")
            logger.info(f"Total entries: {len(funding_rates)}")
            logger.info(f"Date range: {datetime.fromtimestamp(first_entry.get('time', 0)/1000)} "
                       f"to {datetime.fromtimestamp(last_entry.get('time', 0)/1000)}")
            logger.info(f"Output file: {csv_filename}")
            
            # Calculate some statistics
            funding_rates_values = [float(entry.get('fundingRate', 0)) for entry in funding_rates]
            if funding_rates_values:
                avg_rate = sum(funding_rates_values) / len(funding_rates_values)
                min_rate = min(funding_rates_values)
                max_rate = max(funding_rates_values)
                
                logger.info(f"Average funding rate: {avg_rate:.6f}")
                logger.info(f"Min funding rate: {min_rate:.6f}")
                logger.info(f"Max funding rate: {max_rate:.6f}")
        else:
            logger.warning("No funding rate data found for the specified time range")
        
        return csv_filename


def main():
    """
    Main function to run the HYPE historical funding rate fetcher.
    """
    try:
        # Initialize the fetcher
        fetcher = HYPEHistoricalFundingRateFetcher()
        
        # Fetch and save historical funding rates
        csv_filename = fetcher.fetch_and_save_historical_funding_rates(
            output_filename="hype_historical_funding_rates.csv",
            chunk_days=30  # Fetch in 30-day chunks
        )
        
        print(f"\n✅ Successfully completed! Historical funding rates saved to: {csv_filename}")
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 
