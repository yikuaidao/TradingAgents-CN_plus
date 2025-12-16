"""
MCP Finance Tools

Implements the 17 finance tools defined in FinanceMCP_Tools_Reference.md.
"""
import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from tradingagents.dataflows.manager import DataSourceManager

logger = logging.getLogger(__name__)

# Global manager instance (lazy loaded or initialized here)
_manager = DataSourceManager()

# --- 1. Stock Data ---

def get_stock_data(
    code: str,
    market_type: str = "cn",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    indicators: Optional[str] = None
) -> str:
    """
    获取股票行情数据 (开盘价, 最高价, 最低价, 收盘价, 成交量) 及技术指标。
    
    本工具会自动识别股票类型 (A股/港股/美股) 并调用最佳数据源。
    优先从数据库获取数据，若数据库无数据则从数据源拉取并存入数据库。
    
    Args:
        code: 股票代码 (例如: "000001.SZ", "AAPL", "00700.HK")。
        market_type: 市场类型: "cn" (A股), "us" (美股), "hk" (港股)。若不指定，将尝试自动推断。
        start_date: 开始日期 (YYYYMMDD)。默认: 1个月前。
        end_date: 结束日期 (YYYYMMDD)。默认: 今天。
        indicators: 技术指标 (例如: "macd(12,26,9) rsi(14)")。
        
    Returns:
        Markdown 格式的股票数据表格。
    """
    try:
        from tradingagents.utils.stock_utils import StockUtils
        
        # 1. 自动推断市场类型 (优先使用 StockUtils)
        market_info = StockUtils.get_market_info(code)
        is_china = market_info['is_china']
        is_hk = market_info['is_hk']
        is_us = market_info['is_us']
        
        # 如果无法识别，回退到参数指定
        if not (is_china or is_hk or is_us):
            if market_type == "hk": is_hk = True
            elif market_type == "us": is_us = True
            else: is_china = True

        # 2. 设置默认日期
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        # 3. 调用统一数据接口 (包含 Write-Through 逻辑)
        if is_china:
            from tradingagents.dataflows.interface import get_china_stock_data_unified
            # 注意: get_china_stock_data_unified 内部可能会处理 indicators
            data = get_china_stock_data_unified(code, start_date, end_date)
            return f"## A股行情数据 ({code})\n{data}"
            
        elif is_hk:
            from tradingagents.dataflows.interface import get_hk_stock_data_unified
            data = get_hk_stock_data_unified(code, start_date, end_date)
            return f"## 港股行情数据 ({code})\n{data}"
            
        elif is_us:
            # from tradingagents.dataflows.providers.us.optimized import get_us_stock_data_cached
            # data = get_us_stock_data_cached(code, start_date, end_date)
            # Use manager to support AKShare fallback
            data = _manager.get_stock_data(code, "us", start_date, end_date)
            return f"## 美股行情数据 ({code})\n{data}"
            
        return "Error: Unknown market type"
        
    except Exception as e:
        logger.error(f"get_stock_data failed: {e}")
        return f"Error: {str(e)}"

# --- 1.1 Unified Stock News ---

def get_stock_news(
    stock_code: str,
    max_news: int = 10
) -> str:
    """
    统一新闻获取工具 - 根据股票代码自动获取相应市场的新闻。
    
    自动识别股票类型（A股/港股/美股）并从最佳数据源获取新闻：
    - A股: 数据库缓存、东方财富实时新闻、Google中文搜索
    - 港股: Google搜索、实时行情资讯
    - 美股: OpenAI全球新闻、Google英文搜索、FinnHub数据
    
    优先从数据库获取数据，若数据库无数据则从数据源拉取并存入数据库。
    
    Args:
        stock_code: 股票代码，支持多种格式：
            - A股：如 '600519', '000001', '300750'
            - 港股：如 '0700.HK', '09988', '01810.HK'
            - 美股：如 'AAPL', 'TSLA', 'NVDA'
        max_news: 获取新闻的最大数量，建议范围 5-20，默认 10
    
    Returns:
        格式化的新闻内容，包含新闻标题、来源、时间和摘要
    """
    try:
        from tradingagents.tools.mcp.tools import news
        return news.get_stock_news(stock_code, max_news)
    except Exception as e:
        logger.error(f"get_stock_news failed: {e}")
        return f"Error: {str(e)}"

def get_stock_fundamentals(
    ticker: str,
    curr_date: str = None,
    start_date: str = None,
    end_date: str = None
) -> str:
    """
    统一股票基本面分析工具 - 获取股票的财务数据和估值指标。
    
    自动识别股票类型（A股/港股/美股）并调用最佳数据源。
    
    Args:
        ticker: 股票代码
        curr_date: 当前日期（可选）
        start_date: 开始日期（可选）
        end_date: 结束日期（可选）
    
    Returns:
        格式化的基本面分析数据
    """
    try:
        from tradingagents.tools.mcp.tools import fundamentals
        return fundamentals.get_stock_fundamentals(ticker, curr_date, start_date, end_date)
    except Exception as e:
        logger.error(f"get_stock_fundamentals failed: {e}")
        return f"Error: {str(e)}"

def get_stock_sentiment(
    ticker: str,
    curr_date: str,
    start_date: str = None,
    end_date: str = None,
    source_name: str = None
) -> str:
    """
    统一股票情绪分析工具 - 获取市场对股票的情绪倾向。
    
    自动识别股票类型并调用相应数据源。
    
    Args:
        ticker: 股票代码
        curr_date: 当前日期，格式：YYYY-MM-DD
        start_date: 开始日期（可选）
        end_date: 结束日期（可选）
        source_name: 指定数据源名称（可选）
    
    Returns:
        格式化的情绪分析数据
    """
    try:
        from tradingagents.tools.mcp.tools import sentiment
        return sentiment.get_stock_sentiment(ticker, curr_date, start_date, end_date, source_name)
    except Exception as e:
        logger.error(f"get_stock_sentiment failed: {e}")
        return f"Error: {str(e)}"

def get_china_market_overview(
    date: str = None,
    include_indices: bool = True,
    include_sectors: bool = True
) -> str:
    """
    中国A股市场概览工具 - 获取中国A股市场的整体概况。
    
    提供市场指数、板块表现、资金流向等宏观市场数据。
    
    Args:
        date: 查询日期（可选，默认为今天）
        include_indices: 是否包含主要指数数据
        include_sectors: 是否包含板块表现数据
    
    Returns:
        格式化的市场概览数据
    """
    try:
        from tradingagents.tools.mcp.tools import china
        return china.get_china_market_overview(date, include_indices, include_sectors)
    except Exception as e:
        logger.error(f"get_china_market_overview failed: {e}")
        return f"Error: {str(e)}"

# --- 2. Company Performance ---
# (已废弃细分接口，请使用统一的 get_stock_fundamentals)
# --- 1.2 Unified Stock Market Data (DEPRECATED: Merged into get_stock_data) ---

def get_stock_market_data(
    ticker: str,
    start_date: str,
    end_date: str
) -> str:
    """
    统一股票市场数据工具 - 获取股票的历史价格、技术指标和市场表现。
    
    自动识别股票类型（A股/港股/美股）并调用最佳数据源：
    - A股: Tushare、AKShare
    - 港股: AKShare
    - 美股: FinnHub、yfinance
    
    返回数据包括：K线数据、移动平均线、MACD、RSI、布林带等技术指标。
    优先从数据库获取数据，若数据库无数据则从数据源拉取并存入数据库。
    
    Args:
        ticker: 股票代码，支持多种格式：
            - A股：如 '600519', '000001', '300750'
            - 港股：如 '0700.HK', '09988'
            - 美股：如 'AAPL', 'TSLA', 'NVDA'
        start_date: 开始日期，格式：YYYY-MM-DD
        end_date: 结束日期，格式：YYYY-MM-DD
    
    Returns:
        格式化的市场数据，包含K线、技术指标等
    """
    try:
        from tradingagents.tools.mcp.tools import market
        return market.get_stock_market_data(ticker, start_date, end_date)
    except Exception as e:
        logger.error(f"get_stock_market_data failed: {e}")
        return f"Error: {str(e)}"

def get_stock_data_minutes(
    market_type: str,
    code: str,
    start_datetime: str,
    end_datetime: str,
    freq: str
) -> str:
    """
    获取分钟级 K 线数据。
    
    Args:
        market_type: 市场类型 (目前仅支持 "cn")。
        code: 股票代码 (例如: "600519.SH")。
        start_datetime: 开始时间 (YYYY-MM-DD HH:mm:ss 或 YYYYMMDDHHmmss)。
        end_datetime: 结束时间。
        freq: 频率: "1min", "5min", "15min", "30min", "60min"。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_stock_data_minutes(
            market_type=market_type,
            code=code,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            freq=freq
        )
        return _format_result(data, f"{code} {freq} Data")
    except Exception as e:
        logger.error(f"get_stock_data_minutes failed: {e}")
        return f"Error: {str(e)}"

# --- 2. Company Performance ---

def get_company_performance(
    ts_code: str,
    data_type: str,
    start_date: str,
    end_date: str,
    period: Optional[str] = None
) -> str:
    """
    获取 A 股公司业绩/财务数据。
    
    Args:
        ts_code: 股票代码 (例如: "000001.SZ")。
        data_type: 数据类型 (forecast-业绩预告, express-业绩快报, indicators-财务指标, dividend-分红送转, mainbz-主营业务, holder_number-股东人数, holder_trade-股东增减持, managers-管理层, audit-审计意见, company_basic-公司基本信息, balance_basic-资产负债表(基础), balance_all-资产负债表(全部), cashflow_basic-现金流量表(基础), cashflow_all-现金流量表(全部), income_basic-利润表(基础), income_all-利润表(全部), share_float-解禁数据, repurchase-回购数据, top10_holders-前十大股东, top10_floatholders-前十大流通股东, pledge_stat-质押统计, pledge_detail-质押详情)。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        period: 报告期 (YYYYMMDD, 可选)。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_company_performance(
            ts_code=ts_code,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            period=period,
            market="cn"
        )
        return _format_result(data, f"{ts_code} {data_type}")
    except Exception as e:
        logger.error(f"get_company_performance failed: {e}")
        return f"Error: {str(e)}"

def get_company_performance_hk(
    ts_code: str,
    data_type: str,
    start_date: str,
    end_date: str,
    period: Optional[str] = None,
    ind_name: Optional[str] = None
) -> str:
    """
    获取港股财务数据。
    
    Args:
        ts_code: 港股代码 (例如: "00700.HK")。
        data_type: 数据类型 (income-利润表, balance-资产负债表, cashflow-现金流量表)。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        period: 报告期。
        ind_name: 指标名称过滤。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_company_performance(
            ts_code=ts_code,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            period=period,
            ind_name=ind_name,
            market="hk"
        )
        return _format_result(data, f"{ts_code} {data_type} (HK)")
    except Exception as e:
        logger.error(f"get_company_performance_hk failed: {e}")
        return f"Error: {str(e)}"

def get_company_performance_us(
    ts_code: str,
    data_type: str,
    start_date: str,
    end_date: str,
    period: Optional[str] = None
) -> str:
    """
    获取美股财务数据。
    
    Args:
        ts_code: 美股代码 (例如: "AAPL")。
        data_type: 数据类型 (income-利润表, balance-资产负债表, cashflow-现金流量表, indicator-财务指标)。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        period: 报告期。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_company_performance(
            ts_code=ts_code,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            period=period,
            market="us"
        )
        return _format_result(data, f"{ts_code} {data_type} (US)")
    except Exception as e:
        logger.error(f"get_company_performance_us failed: {e}")
        return f"Error: {str(e)}"

# --- 3. Macro & Flows ---

def get_macro_econ(
    indicator: str,
    start_date: str,
    end_date: str
) -> str:
    """
    获取宏观经济数据。
    
    Args:
        indicator: 指标名称 (shibor-Shibor利率, lpr-LPR利率, gdp-GDP数据, cpi-CPI数据, ppi-PPI数据, cn_m-货币供应量, cn_pmi-PMI数据, cn_sf-社融数据, shibor_quote-Shibor报价, libor-Libor利率, hibor-Hibor利率)。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_macro_econ(indicator=indicator, start_date=start_date, end_date=end_date)
        return _format_result(data, f"Macro: {indicator}")
    except Exception as e:
        logger.error(f"get_macro_econ failed: {e}")
        return f"Error: {str(e)}"

def get_money_flow(
    start_date: str,
    end_date: str,
    query_type: Optional[str] = None,
    ts_code: Optional[str] = None,
    content_type: Optional[str] = None,
    trade_date: Optional[str] = None
) -> str:
    """
    获取资金流向数据。
    
    Args:
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        query_type: 查询类型 (stock-个股, market-大盘, sector-板块)。
        ts_code: 股票或板块代码。
        content_type: 板块类型 (industry-行业, concept-概念, area-地域)，仅在 query_type 为 sector 时有效。
        trade_date: 指定交易日期。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_money_flow(
            start_date=start_date,
            end_date=end_date,
            query_type=query_type,
            ts_code=ts_code,
            content_type=content_type,
            trade_date=trade_date
        )
        return _format_result(data, f"Money Flow: {ts_code or query_type}")
    except Exception as e:
        logger.error(f"get_money_flow failed: {e}")
        return f"Error: {str(e)}"

def get_margin_trade(
    data_type: str,
    start_date: str,
    end_date: Optional[str] = None,
    ts_code: Optional[str] = None,
    exchange: Optional[str] = None
) -> str:
    """
    获取融资融券数据。
    
    Args:
        data_type: 数据类型 (margin_secs-融资融券标的, margin-融资融券交易汇总, margin_detail-融资融券交易明细, slb_len_mm-转融通)。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        ts_code: 股票代码。
        exchange: 交易所 (SSE-上交所, SZSE-深交所, BSE-北交所)。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_margin_trade(
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            ts_code=ts_code,
            exchange=exchange
        )
        return _format_result(data, f"Margin Trade: {data_type}")
    except Exception as e:
        logger.error(f"get_margin_trade failed: {e}")
        return f"Error: {str(e)}"

# --- 4. Funds ---

def get_fund_data(
    ts_code: str,
    data_type: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    period: Optional[str] = None
) -> str:
    """
    获取公募基金数据。
    
    Args:
        ts_code: 基金代码。
        data_type: 数据类型 (basic-基本信息, manager-基金经理, nav-净值数据, dividend-分红数据, portfolio-持仓数据, all-全部)。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        period: 报告期。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_fund_data(
            ts_code=ts_code,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
        return _format_result(data, f"Fund: {ts_code} {data_type}")
    except Exception as e:
        logger.error(f"get_fund_data failed: {e}")
        return f"Error: {str(e)}"

def get_fund_manager_by_name(
    name: str,
    ann_date: Optional[str] = None
) -> str:
    """
    根据姓名获取基金经理信息。
    
    Args:
        name: 基金经理姓名。
        ann_date: 公告日期 (YYYYMMDD)。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_fund_manager_by_name(name=name, ann_date=ann_date)
        return _format_result(data, f"Manager: {name}")
    except Exception as e:
        logger.error(f"get_fund_manager_by_name failed: {e}")
        return f"Error: {str(e)}"

# --- 5. Index & Others ---

def get_index_data(
    code: str,
    start_date: str,
    end_date: str
) -> str:
    """
    获取指数日线行情。
    
    Args:
        code: 指数代码 (例如: 000001.SH)。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_index_data(code=code, start_date=start_date, end_date=end_date)
        return _format_result(data, f"Index: {code}")
    except Exception as e:
        logger.error(f"get_index_data failed: {e}")
        return f"Error: {str(e)}"

def get_csi_index_constituents(
    index_code: str,
    start_date: str,
    end_date: str
) -> str:
    """
    获取中证指数成份股及权重。
    
    Args:
        index_code: 指数代码。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_csi_index_constituents(index_code=index_code, start_date=start_date, end_date=end_date)
        return _format_result(data, f"CSI Constituents: {index_code}")
    except Exception as e:
        logger.error(f"get_csi_index_constituents failed: {e}")
        return f"Error: {str(e)}"

def get_convertible_bond(
    data_type: str,
    ts_code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> str:
    """
    获取可转债数据。
    
    Args:
        data_type: 数据类型 (issue-发行信息, info-基本信息)。
        ts_code: 转债代码。
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_convertible_bond(
            data_type=data_type,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date
        )
        return _format_result(data, f"CB: {data_type}")
    except Exception as e:
        logger.error(f"get_convertible_bond failed: {e}")
        return f"Error: {str(e)}"

def get_block_trade(
    start_date: str,
    end_date: str,
    code: Optional[str] = None
) -> str:
    """
    获取大宗交易数据。
    
    Args:
        start_date: 开始日期 (YYYYMMDD)。
        end_date: 结束日期 (YYYYMMDD)。
        code: 股票代码。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_block_trade(start_date=start_date, end_date=end_date, code=code)
        return _format_result(data, f"Block Trade: {code or 'All'}")
    except Exception as e:
        logger.error(f"get_block_trade failed: {e}")
        return f"Error: {str(e)}"

def get_dragon_tiger_inst(
    trade_date: str,
    ts_code: Optional[str] = None
) -> str:
    """
    获取龙虎榜机构明细。
    
    Args:
        trade_date: 交易日期 (YYYYMMDD)。
        ts_code: 股票代码。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_dragon_tiger_inst(trade_date=trade_date, ts_code=ts_code)
        return _format_result(data, f"Dragon Tiger: {trade_date}")
    except Exception as e:
        logger.error(f"get_dragon_tiger_inst failed: {e}")
        return f"Error: {str(e)}"

# --- 6. News ---

def get_finance_news(
    query: str
) -> str:
    """
    搜索财经新闻。
    
    Args:
        query: 搜索关键词。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_finance_news(query=query)
        return _format_result(data, f"News: {query}")
    except Exception as e:
        logger.error(f"get_finance_news failed: {e}")
        return f"Error: {str(e)}"

def get_hot_news_7x24(
    limit: int = 100
) -> str:
    """
    获取 7x24 小时全球财经快讯。
    
    Args:
        limit: 获取条数。
        
    Returns:
        Markdown 格式的表格数据。
    """
    try:
        data = _manager.get_hot_news_7x24(limit=limit)
        return _format_result(data, "Hot News 7x24")
    except Exception as e:
        logger.error(f"get_hot_news_7x24 failed: {e}")
        return f"Error: {str(e)}"

def get_current_timestamp(
    format: str = "%Y-%m-%d %H:%M:%S"
) -> str:
    """
    获取当前时间戳。
    
    Args:
        format: 格式字符串。
        
    Returns:
        当前时间戳。
    """
    return datetime.now().strftime(format)

# --- Helpers ---

def _format_result(data: Any, title: str) -> str:
    """Format data to Markdown"""
    if data is None:
        return f"# {title}\n\nNo data found."
    
    if isinstance(data, list) and not data:
        return f"# {title}\n\nNo data found."
        
    if isinstance(data, str):
        return data
        
    # Assuming data is a list of dicts or a pandas DataFrame (converted to list of dicts)
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        # Create markdown table
        headers = list(data[0].keys())
        header_row = "| " + " | ".join(headers) + " |"
        separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"
        
        rows = []
        for item in data:
            row = "| " + " | ".join([str(item.get(h, "")) for h in headers]) + " |"
            rows.append(row)
            
        return f"# {title}\n\n{header_row}\n{separator_row}\n" + "\n".join(rows)
    
    return f"# {title}\n\n{str(data)}"
