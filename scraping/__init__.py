"""
Scraping module for price monitoring project
"""

# Import from submodules
from .jumia import JumiaScraper
from .marjanemall import MarjanemallScraper

__all__ = ['JumiaScraper', 'MarjanemallScraper']
