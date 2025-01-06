use reqwest::Error;
use serde::Deserialize;
use serde_json::Value;

// 定义交易对信息结构
#[derive(Debug, Deserialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
}

// 获取现货交易对信息
pub async fn get_spot_pairs_info() -> Result<Vec<SymbolInfo>, Error> {
    // Binance API端点
    let url = "https://api.binance.com/api/v3/exchangeInfo";
    
    // 发送HTTP GET请求
    let response = reqwest::get(url).await?;
    
    // 解析JSON响应
    let json: Value = response.json().await?;
    
    // 提取并过滤交易对信息
    let symbols = json["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            s["status"].as_str().unwrap() == "TRADING" &&  // 状态为TRADING
            s["quoteAsset"].as_str().unwrap() == "USDT"    // 报价资产为USDT
        })
        .map(|s| SymbolInfo {
            symbol: s["symbol"].as_str().unwrap().to_string(),
            base_asset: s["baseAsset"].as_str().unwrap().to_string(),
            quote_asset: s["quoteAsset"].as_str().unwrap().to_string(),
            status: s["status"].as_str().unwrap().to_string(),
        })
        .collect();
    
    Ok(symbols)
}