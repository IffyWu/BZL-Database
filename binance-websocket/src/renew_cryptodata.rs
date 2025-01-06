use clap::{Arg, Command};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value;
use std::error::Error;
use log::{info, warn, error};
use env_logger;
use tokio::sync::mpsc;
use tokio::task;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter};
use std::path::Path;

// 定义K线数据结构
#[derive(Debug, Clone)]
struct Kline {
    time: i64,    // 时间戳
    open: f64,    // 开盘价
    high: f64,    // 最高价
    low: f64,     // 最低价
    close: f64,   // 收盘价
    volume: f64,  // 成交量
}

// 保存数据到CSV文件
async fn save_to_csv(symbol: &str, klines: Vec<Kline>) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 创建data目录（如果不存在）
    let dir_path = format!("data/{}", symbol);
    std::fs::create_dir_all(&dir_path)?;
    
    // 生成文件名
    let file_path = format!("{}/{}.csv", dir_path, chrono::Local::now().format("%Y-%m-%d"));
    
    // 打开文件（追加模式）
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(&file_path)?;
    
    let mut writer = BufWriter::new(file);
    
    // 如果文件为空，写入表头
    if std::fs::metadata(&file_path)?.len() == 0 {
        writeln!(writer, "time,open,high,low,close,volume")?;
    }
    
    // 写入数据
    for kline in klines {
        writeln!(
            writer,
            "{},{},{},{},{},{}",
            kline.time,
            kline.open,
            kline.high,
            kline.low,
            kline.close,
            kline.volume
        )?;
    }
    
    writer.flush()?;
    info!("成功保存{}条数据到{}", klines.len(), file_path);
    Ok(())
}

// 获取K线数据
async fn download_kline_data(
    client: &Client,
    symbol: &str,
    start_time: i64,
    end_time: i64,
    interval: &str,
) -> Result<Vec<Kline>, Box<dyn Error + Send + Sync>> {
    let mut klines: Vec<Kline> = Vec::new();
    let mut current_time = start_time;
    
    let mut retry_count = 0;
    const MAX_RETRIES: u8 = 5;
    
    let mut iteration_count = 0;
    const MAX_ITERATIONS: u32 = 1000; // 最大迭代次数
    
    while current_time < end_time && iteration_count < MAX_ITERATIONS {
        iteration_count += 1;
        
        if retry_count >= MAX_RETRIES {
            error!("达到最大重试次数，退出程序");
            break;
        }
        
        info!("当前迭代次数: {}, 当前时间: {}", iteration_count,
            DateTime::from_timestamp(current_time / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S"));
        
        let url = format!(
            "https://api.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&limit=1000",
            symbol, interval, current_time
        );
        
        info!("正在获取数据，时间范围: {} - {}",
            DateTime::from_timestamp(current_time / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S"),
            DateTime::from_timestamp(end_time / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
        );
        
        let response = match client.get(&url).send().await {
            Ok(res) => {
                info!("成功获取响应，状态码: {}", res.status());
                res
            },
            Err(e) => {
                error!("请求失败: {}, 5秒后重试...", e);
                retry_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let response_text = response.text().await?;
        
        let json: Value = match serde_json::from_str(&response_text) {
            Ok(v) => v,
            Err(e) => {
                error!("JSON解析失败: {}, 响应内容: {}", e, response_text);
                retry_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        
        let klines_array = match json.as_array() {
            Some(arr) if !arr.is_empty() => arr,
            _ => {
                // 删除最后获取的不完整数据
                if let Some(last_kline) = klines.last().cloned() {
                    warn!("检测到未完成的K线数据，删除最后一条记录并等待...");
                    klines.pop();
                    current_time = last_kline.time;
                }
                
                // 计算下一个时间点（当前时间戳 + 间隔 + 5秒缓冲）
                let next_time = match interval {
                    "1d" => current_time + 86400 * 1000 + 5000,
                    "1h" => current_time + 3600 * 1000 + 5000,
                    "15m" => current_time + 900 * 1000 + 5000,
                    _ => current_time + 60 * 1000 + 5000,
                };
                
                // 计算需要等待的时间（毫秒）
                let wait_time = next_time - Utc::now().timestamp_millis();
                
                if wait_time > 0 {
                    info!("等待{}秒后重新获取数据...", wait_time / 1000);
                    tokio::time::sleep(std::time::Duration::from_millis(wait_time as u64)).await;
                } else {
                    info!("无需等待，立即重新获取数据");
                }
                continue;
            }
        };
        
        for kline in klines_array {
            let kline = Kline {
                time: kline[0].as_i64().unwrap(),
                open: kline[1].as_str().unwrap().parse().unwrap(),
                high: kline[2].as_str().unwrap().parse().unwrap(),
                low: kline[3].as_str().unwrap().parse().unwrap(),
                close: kline[4].as_str().unwrap().parse().unwrap(),
                volume: kline[5].as_str().unwrap().parse().unwrap(),
            };
            
            if kline.time > end_time {
                break;
            }
            
            klines.push(kline);
        }
        
        // 更新current_time
        if let Some(last_kline) = klines.last() {
            // 对于日线数据，每次增加24小时
            if interval == "1d" {
                current_time = last_kline.time + 86400 * 1000;
            } else {
                current_time = last_kline.time + 1;
            }
        } else {
            // 如果没有获取到数据，根据间隔增加时间
            match interval {
                "1d" => current_time += 86400 * 1000, // 增加一天
                "1h" => current_time += 3600 * 1000,  // 增加一小时
                _ => current_time += 60 * 1000,       // 默认增加一分钟
            }
        }
        
        // 防止无限循环
        if current_time >= end_time {
            info!("达到结束时间，停止获取数据");
            break;
        }
        
        // 添加调试信息
        info!("更新时间: {} -> {}",
            DateTime::from_timestamp((current_time - 1) / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S"),
            DateTime::from_timestamp(current_time / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
        );
    }
    
    Ok(klines)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 初始化日志
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    info!("程序启动，开始持续获取数据...");

    // 解析命令行参数
    let matches = Command::new("renew_cryptodata")
        .version("1.0")
        .author("Your Name")
        .about("持续获取加密货币数据并保存到数据库")
        .arg(Arg::new("symbols")
            .help("交易对列表，用逗号分隔，例如BTCUSDT,ETHUSDT")
            .required(true)
            .index(1))
        .arg(Arg::new("interval")
            .help("K线周期，例如1m, 5m, 15m, 1h, 1d")
            .required(true)
            .index(2))
        .arg(Arg::new("db_url")
            .help("数据库连接URL")
            .required(true)
            .index(3))
        .get_matches();

    // 初始化数据库连接池
    let db_pool = init_db_pool(matches.get_one::<String>("db_url").unwrap()).await?;
    let state = Arc::new(AppState { db_pool });

    // 解析交易对列表
    let symbols: Vec<String> = matches.get_one::<String>("symbols").unwrap().split(',').map(|s| s.to_string()).collect();
    let interval = matches.get_one::<String>("interval").unwrap();

    // 创建channel用于任务分发
    let (tx, mut rx) = mpsc::channel(32);

    // 启动任务分发器
    task::spawn(async move {
        while let Some((symbol, klines)) = rx.recv().await {
            task::spawn(async move {
                if let Err(e) = save_to_csv(&symbol, klines).await {
                    error!("保存数据到CSV失败: {}", e);
                }
            });
        }
    });

    // 创建HTTP客户端
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // 启动数据获取任务
    for symbol in symbols {
        let client = client.clone();
        let tx = tx.clone();
        let interval = interval.clone();
        
        task::spawn(async move {
            // 获取该交易对的最后时间戳
            let state_file = format!("data/{}/.state", symbol);
            let mut last_time = match std::fs::read_to_string(&state_file) {
                Ok(content) => {
                    let timestamp = content.trim().parse::<i64>().unwrap_or_else(|_| {
                        warn!("状态文件内容无效，使用24小时前时间");
                        Utc::now().timestamp_millis() - 86400 * 1000
                    });
                    info!("从状态文件读取{}最后时间戳: {}", symbol,
                        DateTime::from_timestamp(timestamp / 1000, 0)
                            .unwrap()
                            .format("%Y-%m-%d %H:%M:%S"));
                    timestamp
                },
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    info!("状态文件不存在，使用24小时前时间");
                    Utc::now().timestamp_millis() - 86400 * 1000
                },
                Err(e) => {
                    error!("读取状态文件失败: {}, 使用24小时前时间", e);
                    Utc::now().timestamp_millis() - 86400 * 1000
                }
            };
            
            // 缓存最后时间戳，避免频繁查询数据库
            let mut last_timestamp_cache = last_time;
            
            loop {
                let end_time = Utc::now().timestamp_millis();
                
                // 每10分钟检查一次最新时间戳
                if last_timestamp_cache + 600_000 < end_time {
                    match get_last_timestamp(&clickhouse_client, &symbol).await {
                        Ok(t) => {
                            if t > last_timestamp_cache {
                                info!("{} 更新最后时间戳: {}", symbol,
                                    DateTime::from_timestamp(t / 1000, 0)
                                        .unwrap()
                                        .format("%Y-%m-%d %H:%M:%S"));
                                last_timestamp_cache = t;
                            }
                        },
                        Err(e) => {
                            error!("更新{}最后时间戳失败: {}", symbol, e);
                        }
                    }
                }
                
                match download_kline_data(&client, &symbol, last_timestamp_cache + 1, end_time, &interval).await {
                    Ok(klines) => {
                        if !klines.is_empty() {
                            if let Err(e) = tx.send((symbol.clone(), klines.clone())).await {
                                error!("发送数据到channel失败: {}", e);
                            }
                            last_time = klines.last().unwrap().time;
                        }
                    }
                    Err(e) => {
                        error!("获取数据失败: {}", e);
                    }
                }
                
                // 根据间隔等待
                let wait_time = match interval.as_str() {
                    "1m" => 60,
                    "5m" => 300,
                    "15m" => 900,
                    "1h" => 3600,
                    "1d" => 86400,
                    _ => 60,
                };
                
                tokio::time::sleep(std::time::Duration::from_secs(wait_time)).await;
            }
        });
    }

    // 等待所有任务完成
    tokio::signal::ctrl_c().await?;
    info!("收到终止信号，程序退出");
    Ok(())
}