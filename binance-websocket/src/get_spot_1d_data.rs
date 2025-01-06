use chrono::Utc;
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use log::{error, info};
use binance_websocket::clickhouse_connect::create_client;
use env_logger;

// K线数据结构
#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
struct Kline {
    open_time: i64,  // 开盘时间（毫秒级时间戳）
    open: f64,                // 开盘价
    high: f64,                // 最高价
    low: f64,                 // 最低价
    close: f64,               // 收盘价
    volume: f64,              // 成交量
    close_time: i64, // 收盘时间（毫秒级时间戳）
    quote_asset_volume: f64,  // 报价资产成交量
    number_of_trades: u64,    // 交易笔数
    taker_buy_base_volume: f64, // 主动买入成交量
    taker_buy_quote_volume: f64 // 主动买入报价成交量
}

// 创建1d_data表
async fn create_table(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // 创建数据库
    client.query("CREATE DATABASE IF NOT EXISTS binance_1d_spot_data").execute().await?;
    
    // 创建表
    let ddl = r"
        CREATE TABLE IF NOT EXISTS binance_1d_spot_data.BTCUSDT (
            open_time DateTime,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            close_time DateTime,
            quote_asset_volume Float64,
            number_of_trades UInt64,
            taker_buy_base_volume Float64,
            taker_buy_quote_volume Float64
        ) ENGINE = MergeTree()
        ORDER BY open_time";
    
    // 连接已通过Client::default()创建
    client.query(ddl).execute().await?;
    Ok(())
}

// 获取历史K线数据
async fn get_historical_klines() -> Result<Vec<Kline>, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let url = "https://api.binance.com/api/v3/klines";
    
    // 获取过去30天的数据
    let end_time = Utc::now();
    let start_time = (end_time.timestamp_millis() - 30 * 24 * 60 * 60 * 1000).to_string();  // 30天前的时间戳
    let end_time = end_time.timestamp_millis().to_string();
    
    let params = [
        ("symbol", "BTCUSDT"),
        ("interval", "1d"),
        ("startTime", &start_time.to_string()),
        ("endTime", &end_time.to_string()),
        ("limit", "30")
    ];
    
    let response = client.get(url)
        .query(&params)
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .send()
        .await?;
        
    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await?;
        return Err(format!("API请求失败: {} - {}", status, error_text).into());
    }
    
    let response_text = response.text().await?;
    log::debug!("API响应: {}", response_text);
    let klines: Vec<Vec<serde_json::Value>> = serde_json::from_str(&response_text)?;
    
    let mut result = Vec::new();
    for (i, k) in klines.iter().enumerate() {
        if k.len() < 11 {
            log::error!("第{}条K线数据不完整，跳过", i);
            continue;
        }
        
        result.push(Kline {
            open_time: k[0].as_i64().ok_or("无法解析open_time")?,
            open: k[1].as_str().ok_or("无法解析open")?.parse()?,
            high: k[2].as_str().ok_or("无法解析high")?.parse()?,
            low: k[3].as_str().ok_or("无法解析low")?.parse()?,
            close: k[4].as_str().ok_or("无法解析close")?.parse()?,
            volume: k[5].as_str().ok_or("无法解析volume")?.parse()?,
            close_time: k[6].as_i64().ok_or("无法解析close_time")?,
            quote_asset_volume: k[7].as_str().ok_or("无法解析quote_asset_volume")?.parse()?,
            number_of_trades: k[8].as_u64().ok_or("无法解析number_of_trades")?,
            taker_buy_base_volume: k[9].as_str().ok_or("无法解析taker_buy_base_volume")?.parse()?,
            taker_buy_quote_volume: k[10].as_str().ok_or("无法解析taker_buy_quote_volume")?.parse()?,
        });
    }
    
    Ok(result)
}

// 插入数据到ClickHouse
async fn insert_klines(client: &Client, klines: Vec<Kline>) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client.insert("binance_1d_spot_data.BTCUSDT")?;
    for kline in klines {
        insert.write(&kline).await?;
    }
    insert.end().await?;
    // 验证数据插入
    let count: u64 = client
        .query("SELECT count() FROM binance_1d_spot_data.BTCUSDT")
        .fetch_one()
        .await?;
        
    log::info!("成功插入{}条K线数据", count);
    Ok(())
}

// 主函数
use clap::{Arg, Command};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("binance-websocket")
        .version("0.1.0")
        .about("Binance websocket data collector")
        .arg(Arg::new("drop-table")
            .long("drop-table")
            .action(clap::ArgAction::SetTrue)
            .help("Drop existing table before starting"))
        .get_matches();
    // 初始化日志
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    
    // 初始化数据库连接池
    let client = create_client();

    // 如果指定了 --drop-table 参数，先删除表
    if matches.get_flag("drop-table") {
        client.query("DROP TABLE IF EXISTS binance_1d_spot_data.BTCUSDT")
            .execute()
            .await?;
        log::info!("成功删除表 binance_1d_spot_data.BTCUSDT");
    }
    
    // 创建表
    create_table(&client).await?;

    // 获取历史数据并插入
    match get_historical_klines().await {
        Ok(klines) => {
            log::info!("成功获取{}条历史K线数据", klines.len());
            if let Err(e) = insert_klines(&client, klines).await {
                log::error!("插入历史数据失败: {}", e);
            }
        }
        Err(e) => log::error!("获取历史数据失败: {}", e),
    }

    // 持续运行，定期获取最新数据
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        
        match get_historical_klines().await {
            Ok(klines) => {
                if !klines.is_empty() {
                    log::info!("获取到{}条最新K线数据", klines.len());
                    if let Err(e) = insert_klines(&client, klines).await {
                        log::error!("插入最新数据失败: {}", e);
                    }
                }
            }
            Err(e) => log::error!("获取最新数据失败: {}", e),
        }
    }
}