# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a market potential analysis tool for evaluating overseas market performance of Chinese short dramas. The system scrapes ADX platform data to help prioritize which dramas to launch and how much resources to allocate.

**Business Context**: The company acquired distribution rights for 9 Chinese short dramas and needs to determine launch order and investment levels (S/A/B/C tier) based on historical market performance data.

## Core Architecture

### Three-Layer Design

1. **Data Collection Layer**
   - ADX platform scraper (primary data source)
   - Multi-language search capability (Chinese → English/Korean/Japanese translations)
   - Fuzzy matching for drama titles (handles localized names)

2. **Data Processing Layer**
   - Time window analysis: 30-day recent trends + 2-year lifecycle data
   - Aggregation by drama, publisher, country, and creative
   - Heat score calculation using weighted formula

3. **Output Layer**
   - Excel report generation with multiple sheets
   - Priority ranking (S/A/B/C) based on decision matrix
   - Data visualization and pivot tables

## Key Data Requirements

### Core Metrics (Must Have)

For each drama, collect data across two time windows:
- **Window A**: Last 30 days (recent heat)
- **Window B**: Last 2 years (lifecycle)

**Required Fields**:
- `impressions`: Total ad impressions
- `creative_count`: Number of ad creatives
- `active_days`: Days with active campaigns
- `country_count`: Number of countries targeted
- `publisher_count`: Number of apps running the drama

### High-Value Dimensions (Priority Order)

1. **Publisher Analysis**: Which apps (ReelShort, DramaBox, FlexTV) are running this drama
   - Identifies S-tier dramas (if major players are investing heavily)
   - Copyright risk detection (avoid conflicts with competitors)

2. **Lifecycle Duration**: First launch date → Last active date
   - Long-running dramas (>180 days) = high LTV, evergreen content
   - Short runs (<7 days) = one-hit wonders, risky

3. **Creative Survival Rate**: % of creatives active >7 days
   - Indicates presence of "viral" creatives

## Output Format

### Excel Structure (3 Sheets)

**Sheet 1: Daily Data (30-day window)**
- Columns: 剧名, 日期, 曝光量, 素材数, 投放国家数, 投放平台数, 主要投放平台
- One row per drama per day for the last 30 days

**Sheet 2: Lifecycle Summary (2-year window)**
- Columns: 剧名, 总曝光量, 总素材数, 投放天数, 首次投放日期, 最后投放日期, 生命周期(天), 覆盖国家总数, 投放平台总数
- One row per drama with aggregated 2-year data

**Sheet 3: Aggregated Analysis & Ranking**
- Columns: 剧名, 近30天曝光量, 2年总曝光量, 生命周期(天), 热度得分, 主要投放平台, 优先级建议
- Includes calculated heat score and priority tier (S/A/B/C)

### Heat Score Formula

```python
heat_score = (
    recent_30d_impressions * 0.4 +
    total_2y_impressions * 0.3 +
    lifecycle_days * 0.2 +
    publisher_count * 0.1
)
```

Weights can be adjusted based on business needs.

## Decision Logic & Priority Matrix

### Priority S (蓝海爆款 - Blue Ocean Hit)

**Characteristics**:
- No or minimal ADX records (未被竞品发现的处女地)
- Or only pirated content, no legitimate publishers
- Theme aligns with current market trends (e.g., revenge, werewolf)

**Strategy**: Heavy investment, first-mover advantage, S-tier marketing resources

### Priority A (验证过的好剧 - Validated Winner)

**Characteristics**:
- High 2-year cumulative impressions (市场已验证吸量能力)
- Lifecycle > 90 days (长期稳定表现)
- BUT low recent 30-day activity (竞品已停投或减少投放)

**Strategy**: "Old drama revival" with new packaging, A-tier resources

### Priority B (观望剧 - Wait-and-See)

**Characteristics**:
- Moderate ADX records (有一定数据但不突出)
- Lifecycle 30-90 days
- Medium impressions, no clear hit signals

**Strategy**: Supplementary content, B-tier resources, test first then scale

### Priority C (红海/烂剧 - Red Ocean / Poor Performer)

**Characteristics**:
- **Case 1 - Red Ocean**: Massive recent 30-day impressions (竞品正在猛砸)
- **Case 2 - One-hit wonder**: Lifecycle < 14 days (一波流就停了)

**Strategy**:
- Red Ocean: Avoid direct competition, delay launch
- Poor performer: Minimal resources, filler content only

## Technical Implementation Notes

### Multi-Language Search Strategy

**Challenge**: Chinese dramas are renamed when going overseas
- Example: 《回家的诱惑》→ "Temptation of Home" or "Fatal Wife"

**Solution**:
- Integrate translation API or LLM for automatic translation
- Generate 3-5 English keyword combinations for fuzzy search
- Support multi-language search (English, Korean, Japanese for major markets)

### Data Source

- **Primary**: ADX platform API or web scraper
- **Fallback**: Alternative ad intelligence platforms if ADX has strict rate limits

### Retry & Error Handling

- Implement exponential backoff for API rate limits
- Handle missing data gracefully (some dramas may have zero records)
- Log all search queries and results for debugging

## Common Commands

### Environment Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.example .env  # Then edit .env file with API credentials
```

### Running the Scraper

```bash
# Basic usage with drama list file
python market_potential_scraper.py --input dramas.txt --output report.xlsx

# Specify custom time windows
python market_potential_scraper.py --recent-days 30 --lifecycle-days 730

# Run with specific drama names (comma-separated)
python market_potential_scraper.py --dramas "回家的诱惑,霸道总裁爱上我,重生之豪门千金"

# Enable debug mode for troubleshooting
python market_potential_scraper.py --input dramas.txt --debug
```

## Environment Variables

Required environment variables in `.env` file:

```bash
# ADX Platform Credentials
ADX_API_KEY=your_api_key_here
ADX_API_SECRET=your_api_secret_here

# Translation API (optional, for multi-language search)
TRANSLATION_API_KEY=your_translation_key_here

# LLM API (optional, for intelligent search query generation)
OPENAI_API_KEY=your_openai_key_here
# or
GEMINI_API_KEY=your_gemini_key_here

# Output Configuration
DEFAULT_OUTPUT_PATH=./reports/
DEFAULT_RECENT_DAYS=30
DEFAULT_LIFECYCLE_DAYS=730
```

## Project Deliverables

1. **Python Script**: `market_potential_scraper.py` - Main scraper script
2. **Configuration File**: `config.json` - Drama list and settings
3. **Output Report**: `9部剧市场潜力分析报告.xlsx` - Excel analysis report
4. **Documentation**: This CLAUDE.md and README.md

## Important Notes

### Data Accuracy
- ADX data may have delays or be incomplete
- Cross-validate with multiple data sources when possible
- Some dramas may have zero records (not necessarily bad - could be blue ocean opportunity)

### Copyright Risk
- If competitors are heavily investing, verify copyright ownership
- Avoid launching if there's potential copyright conflict
- Document all publishers found for each drama for legal review

### Market Dynamics
- Short drama market changes rapidly
- Recommend updating data weekly or monthly
- Recent 30-day data is more important than 2-year data for trending topics

### Compliance
- Respect robots.txt and platform terms of service
- Implement rate limiting to avoid API bans
- Do not scrape personal user data, only aggregate ad metrics

## Key Business Metrics

### Decision Thresholds

| Metric | S-tier | A-tier | B-tier | C-tier |
|--------|--------|--------|--------|--------|
| 2-year impressions | 0 or <1M | >50M | 10M-50M | >100M (red ocean) |
| Recent 30-day | 0 or <100K | <1M | 1M-10M | >10M (red ocean) |
| Lifecycle days | N/A | >90 | 30-90 | <14 |
| Publisher count | 0-1 | 2-5 | 3-8 | >10 |

### Major Publishers to Watch

- **ReelShort**: Top-tier player, if they're running it heavily = S-tier drama
- **DramaBox**: Major competitor, high investment signals
- **FlexTV**: Growing player, moderate signal
- Small/unknown apps: Data may be underestimated, needs manual review
