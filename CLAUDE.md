# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Docker Deployment
```bash
# Start all services with Docker Compose
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f

# Update images and restart
docker-compose pull && docker-compose up -d
```

### Development Setup
```bash
# Install dependencies (recommended approach using uv)
uv pip install -e .
# Or with standard pip
pip install -e .

# Set up environment file
cp .env.example .env
# Edit .env with your API keys and configurations

# Run main trading analysis (CLI version)
python main.py

# Start FastAPI backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Start frontend (from frontend directory)
cd frontend
npm install
npm run dev

# For production build
cd frontend && npm run build
```

### Testing
```bash
# Run all tests
pytest

# Run specific test files
pytest tests/integration/test_mcp_integration.py
pytest tests/mcp/test_basic.py

# Run tests with coverage
pytest --cov=tradingagents

# Run tests for specific modules
pytest tests/unit/
pytest tests/functional/

# Run with verbose output
pytest -v
```

### Database Operations
```bash
# MongoDB operations (via Docker)
docker exec -it tradingagents-mongodb mongosh
docker exec -it tradingagents-mongodb mongosh --username admin --password tradingagents123 --authenticationDatabase admin tradingagents

# Redis operations (via Docker)
docker exec -it tradingagents-redis redis-cli -a tradingagents123
```

## Architecture Overview

### Multi-Agent System Architecture
This is a sophisticated multi-agent stock analysis system with the following key components:

1. **Core Agent Types** (defined in `config/agents/phase1_agents_config.yaml`):
   - **Financial News Analyst** (`财经新闻分析师`): Analyzes latest financial news impact on stock prices with 15-30 minute real-time monitoring.
   - **China Market Analyst** (`中国市场分析师`): Specializes in A-share/HK market analysis with Chinese market characteristics, policy impact assessment, and fund flow analysis.
   - **Market Technical Analyst** (`市场技术分析师`): Provides technical analysis with indicators like MA, MACD, RSI, and comprehensive chart pattern recognition.
   - **Social Media Analyst** (`社交媒体和投资情绪分析师`): Monitors Chinese social media sentiment (Xueqiu, East Money, Weibo) and investment behavior, providing sentiment scoring.
   - **Fundamentals Analyst** (`基本面分析师`): Deep fundamental analysis with valuation models (DCF, PE, PB, PEG), financial health assessment, and industry analysis.
   - **Short-term Capital Analyst** (`短线资金分析师`): Quantitative analysis of capital flows and trading signals using a six-dimensional monitoring system (Main Force, Dragon Tiger List, Northbound, Retail, Margin, Order Book).

2. **Agent Orchestration**:
   - **TradingAgentsGraph** (`tradingagents/graph/trading_graph.py`): Main orchestrator that manages agent workflow using `langgraph`.
   - **DynamicAnalystFactory**: Loads agent configurations from YAML files.
   - **Agent States**: Defines state management for agent interactions.

3. **Data Sources Integration**:
   - **Chinese Market Data**: AkShare, Tushare, BaoStock for A-shares.
   - **International Data**: yFinance, Finnhub for global markets.
   - **Real-time Data**: Multiple APIs with caching via Redis.

4. **Backend Architecture** (FastAPI):
   - **RESTful APIs**: Located in `app/` directory.
   - **Database**: MongoDB for persistent storage, Redis for caching.
   - **Authentication**: JWT-based auth system.
   - **Real-time Updates**: SSE + WebSocket for live analysis progress.

5. **Frontend Architecture** (Vue 3):
   - **Modern SPA**: Vue 3 + Element Plus + Vite.
   - **Components**: Modular components in `frontend/src/components/`.
   - **State Management**: Centralized state management using Pinia.
   - **Real-time UI**: Updates via WebSocket/SSE.

### Key Configuration Files
- **Environment**: `.env` (copy from `.env.example`)
- **Agent Configs**: `config/agents/phase1_agents_config.yaml` (primary), `tradingagents/agents/phase1_agents_config.yaml` (backup)
- **Stock Analysis Configs**: `config/agents/stock_analysis_agents_config.yaml`
- **Docker**: `docker-compose.yml` (production deployment), `docker-compose.hub.nginx.yml` (Docker Hub variant)
- **Dependencies**: `pyproject.toml` (primary)
- **Frontend**: `frontend/package.json` (Node.js dependencies)
- **Database Config**: `tradingagents/config/database_config.py`

### LLM Integration
The system supports multiple LLM providers:
- **OpenAI**: GPT models via official API
- **Google AI**: Gemini models
- **DashScope**: Alibaba's Tongyi Qianwen
- **DeepSeek**: DeepSeek models
- **Anthropic**: Claude models

Configuration is managed through environment variables and the `create_llm_by_provider` function.

### MCP (Model Context Protocol)
The system includes MCP integration for enhanced tool capabilities:
- **FinanceMCP**: Dedicated financial data tools based on Tushare API.
- **MCP Adapters**: LangChain MCP integration (`langchain-mcp-adapters`).
- **Tool Management**: Dynamic tool loading and configuration.

### Data Flow
1. **Input**: Stock symbol and analysis date.
2. **Agent Selection**: Load configured analysts from YAML.
3. **Data Collection**: Gather market data, news, fundamentals using MCP tools and other providers.
4. **Multi-Agent Analysis**: Parallel execution of specialist agents managed by the graph.
5. **Synthesis**: Combine insights into final investment recommendation.
6. **Output**: Structured analysis report with investment advice.

### Development Notes
- **Language**: Primary language is Chinese for analysis outputs.
- **Market Focus**: Optimized for Chinese A-share market characteristics.
- **Currency**: Default currency is RMB for valuations.
- **Timezone**: Asia/Shanghai timezone for market data.
- **Compliance**: Includes risk warnings and compliance features.

### Important Paths
- **Core Logic**: `tradingagents/` - Main analysis engine and multi-agent system.
- **Web Backend**: `app/` - FastAPI application with RESTful APIs.
- **Web Frontend**: `frontend/` - Vue 3 + TypeScript application.
- **Configuration**: `config/` - Agent and system configurations (primary).
- **Trading Configs**: `tradingagents/config/` - Database and provider configurations.
- **Agent Configs**: `tradingagents/agents/` - Backup agent configurations.
- **Data**: `data/` - Local data storage and cache.
- **Logs**: `logs/` - Application logs and analysis results.
- **Docker**: Docker files in root directory.
- **FinanceMCP**: `FinanceMCP/` - Model Context Protocol financial tools server.
- **Documentation**: `docs/` - Project documentation and guides.

### Performance Considerations
- **Caching**: Redis caching for frequently accessed data.
- **Async Processing**: Async/await patterns for concurrent operations.
- **Database Indexing**: Optimized MongoDB queries for historical data.
- **Rate Limiting**: Built-in rate limiting for API calls.

### Security Features
- **JWT Authentication**: Token-based user authentication.
- **CSRF Protection**: Cross-site request forgery prevention.
- **API Key Management**: Secure handling of external API keys.
- **Input Validation**: Pydantic-based data validation.
- **CORS Configuration**: Configurable cross-origin resource sharing.

## FinanceMCP Integration

The system includes FinanceMCP (Model Context Protocol) for enhanced financial data capabilities, located in `FinanceMCP/`.

### MCP Tools List
FinanceMCP provides the following tools:
1.  **stock_data**: Stock/Crypto historical data (A-share, US, HK, FX, Futures, etc.).
2.  **stock_data_minutes**: Minute-level K-line data.
3.  **company_performance**: A-share company financial analysis.
4.  **company_performance_hk**: HK stock company financial analysis.
5.  **company_performance_us**: US stock company financial analysis.
6.  **macro_econ**: Macroeconomic data (GDP, CPI, PPI, PMI, etc.).
7.  **money_flow**: Capital flow data.
8.  **margin_trade**: Margin trading data.
9.  **fund_data**: Mutual fund data.
10. **fund_manager_by_name**: Fund manager query.
11. **index_data**: Index data.
12. **csi_index_constituents**: CSI index constituents and weights.
13. **convertible_bond**: Convertible bond data.
14. **block_trade**: Block trade data.
15. **dragon_tiger_inst**: Dragon Tiger List institutional details.
16. **finance_news**: Financial news search.
17. **hot_news_7x24**: 7x24 hot news.

### MCP Configuration
- **MCP Server**: Located in `FinanceMCP/` directory.
- **Tool Loading**: Dynamic MCP tool loading via `tradingagents/tools/mcp/loader.py`.
- **Configuration**: MCP tools configured through YAML files and environment variables.
- **Integration**: Seamlessly integrated with agent workflows via `langchain-mcp-adapters`.

## Additional Development Notes

### Code Quality and Standards
- **Type Hints**: Comprehensive type annotations throughout the codebase.
- **Error Handling**: Robust error handling with detailed logging.
- **Testing**: Unit tests, integration tests, and MCP-specific tests.
- **Documentation**: Inline documentation and comprehensive guides.

### Performance Optimization
- **Database Indexing**: Optimized MongoDB queries for historical data.
- **Caching Strategy**: Multi-level caching with Redis and application-level cache.
- **Async Operations**: Non-blocking I/O for improved responsiveness.
- **Resource Management**: Efficient memory and CPU usage patterns.

### Internationalization
- **Language Support**: Primary Chinese interface with English fallbacks.
- **Market Coverage**: A-shares, Hong Kong stocks, and US markets.
- **Currency Handling**: Multi-currency support with automatic conversion.
- **Timezone Support**: Proper timezone handling for global markets.
