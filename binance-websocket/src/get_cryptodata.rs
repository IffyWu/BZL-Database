use clap::{Arg, Command};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use log::{error, info, warn};
use env_logger;

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

// 将日期字符串转换为时间戳
fn parse_date(date_str: &str) -> Result<i64, Box<dyn Error>> {
    // 尝试解析完整日期时间
    if let Ok(dt) = DateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S %z") {
        return Ok(dt.timestamp_millis());
    }
    
    // 尝试解析仅日期
    if let Ok(dt) = DateTime::parse_from_str(&format!("{} 00:00:00 +0000", date_str), "%Y-%m-%d %H:%M:%S %z") {
        return Ok(dt.timestamp_millis());
    }
    
    // 如果都失败，返回错误
    Err("无法解析日期格式，请使用YYYY-MM-DD格式".into())
}

// 获取K线数据
async fn download_kline_data(
    client: &Client,
    symbol: &str,
    start_time: i64,
    end_time: i64,
    interval: &str,
) -> Result<Vec<Kline>, Box<dyn Error>> {
    let mut klines: Vec<Kline> = Vec::new();
    let mut current_time = start_time;
    
    let mut retry_count = 0;
    const MAX_RETRIES: u8 = 5;
    
    let mut iteration_count = 0;
    const MAX_ITERATIONS: u32 = 1000; // 最大迭代次数
    
    while current_time < end_time && iteration_count < MAX_ITERATIONS {
        iteration_count += 1;
        
        if retry_count >= MAX_RETRIES {
            eprintln!("达到最大重试次数，退出程序");
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
        
        println!("正在获取数据，时间范围: {} - {}",
            DateTime::from_timestamp(current_time / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S"),
            DateTime::from_timestamp(end_time / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
        );
        
        let response = match client.get(&url).send().await {
            Ok(res) => {
                println!("成功获取响应，状态码: {}", res.status());
                res
            },
            Err(e) => {
                eprintln!("请求失败: {}, 5秒后重试...", e);
                retry_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let response_text = response.text().await?;
        
        let json: Value = match serde_json::from_str(&response_text) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("JSON解析失败: {}, 响应内容: {}", e, response_text);
                retry_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        
        let klines_array = match json.as_array() {
            Some(arr) if !arr.is_empty() => arr,
            _ => {
                // 如果json不是数组或为空，返回空数组
                if json.is_null() || json.as_array().map(|a| a.is_empty()).unwrap_or(true) {
                    return Ok(Vec::new());
                }
                
                // 删除最后获取的不完整数据
                if let Some(last_kline) = klines.last().cloned() {
                    warn!("检测到未完成的K线数据，删除最后一条记录...");
                    klines.pop();
                    current_time = last_kline.time;
                    
                    // 保存当前数据到CSV
                    if let Err(e) = save_to_csv(&klines, symbol) {
                        error!("保存数据失败: {}", e);
                        return Err(e.into());
                    }
                    
                    // 保存状态文件
                    let state_file = format!("data/{}/.state", symbol);
                    if let Err(e) = std::fs::write(&state_file, current_time.to_string()) {
                        error!("保存状态文件失败: {}", e);
                        return Err(e.into());
                    }
                    
                    // 调用renew_cryptodata
                    let renew_process = std::process::Command::new("cargo")
                        .arg("run")
                        .arg("--bin")
                        .arg("renew_cryptodata")
                        .arg("--")
                        .arg(symbol)
                        .arg(interval)
                        .spawn()?;
                    
                    info!("持续获取进程已启动，PID: {}", renew_process.id());
                    return Ok(klines);
                }
                
                // 如果没有最后一条记录，返回空数组
                return Ok(Vec::new());
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

// 保存数据到CSV文件
fn save_to_csv(klines: &[Kline], symbol: &str) -> Result<(), Box<dyn Error>> {
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
    for kline in klines.iter() {
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

// 获取交易对的最早交易时间（使用二分查找优化）
async fn get_first_trade_time(client: &Client, symbol: &str, interval: &str) -> Result<i64, Box<dyn Error>> {
    info!("开始获取{}在{}间隔下的最早交易时间...", symbol, interval);
    
    // 定义初始时间范围
    let mut low = 0; // 最小可能时间（1970-01-01）
    let mut high = Utc::now().timestamp_millis(); // 当前时间
    let mut earliest_time = high;
    
    // 最大尝试次数
    const MAX_ATTEMPTS: u32 = 50;
    let mut attempts = 0;
    
    // 定义时间步长（根据间隔调整）
    let step = match interval {
        "1d" => 86400 * 1000, // 1天
        "1h" => 3600 * 1000,  // 1小时
        "15m" => 900 * 1000,  // 15分钟
        _ => 60000,           // 默认1分钟
    };
    
    while low <= high && attempts < MAX_ATTEMPTS {
        attempts += 1;
        let mid = low + (high - low) / 2;
        
        // 构建请求URL
        let url = format!(
            "https://api.binance.com/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit=1",
            symbol, interval, mid, mid + step
        );
        
        info!("二分查找尝试 {}: 时间范围 {} - {}",
            attempts,
            DateTime::from_timestamp(mid / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S"),
            DateTime::from_timestamp((mid + step) / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
        );
        
        // 发送请求
        let response = match client.get(&url).send().await {
            Ok(res) => res,
            Err(e) => {
                warn!("请求失败: {}, 等待后重试...", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        
        // 解析响应
        let json: Value = match response.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!("JSON解析失败: {}, 等待后重试...", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        
        // 检查是否有数据
        if let Some(data) = json.as_array() {
            if !data.is_empty() {
                // 找到数据，更新最早时间并继续向左查找
                earliest_time = mid;
                high = mid - 1;
            } else {
                // 没有数据，向右查找
                low = mid + 1;
            }
        } else {
            warn!("API返回数据格式错误，等待后重试...");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }
    }
    
    if attempts >= MAX_ATTEMPTS {
        warn!("达到最大尝试次数，可能未找到最早时间");
    }
    
    info!("成功获取{}在{}间隔下的最早交易时间: {}", symbol, interval,
        DateTime::from_timestamp(earliest_time / 1000, 0)
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S"));
    
    Ok(earliest_time)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("程序启动...");
    // 初始化日志
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    info!("程序启动，开始处理请求...");
    
    // 解析命令行参数
    let matches = Command::new("get_cryptodata")
        .version("1.0")
        .author("Your Name")
        .about("从Binance获取加密货币历史数据")
        .arg(Arg::new("symbol")
            .help("交易对，例如BTCUSDT")
            .required(true)
            .index(1))
        .arg(Arg::new("start_date")
            .help("开始日期，格式YYYY-MM-DD 或 earliest")
            .required(true)
            .index(2))
        .arg(Arg::new("end_date")
            .help("结束日期，格式YYYY-MM-DD 或 now")
            .required(true)
            .index(3))
        .arg(Arg::new("interval")
            .help("K线周期，例如1m, 5m, 15m, 1h, 1d")
            .required(true)
            .index(4))
        .arg(Arg::new("renew")
            .help("是否在结束后启动持续获取模式")
            .long("renew")
            .action(clap::ArgAction::SetTrue))
        .get_matches();
    
    // 创建HTTP客户端，设置超时
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // 获取参数值
    let symbol = matches.get_one::<String>("symbol").expect("缺少symbol参数");
    let start_date = matches.get_one::<String>("start_date").expect("缺少start_date参数");
    let end_date = matches.get_one::<String>("end_date").expect("缺少end_date参数");
    let interval = matches.get_one::<String>("interval").expect("缺少interval参数");
    
    // 转换时间
    // 如果start_date为"earliest"，获取最早交易时间
    let start_time = if start_date == "earliest" {
        info!("正在获取{}的最早交易时间...", symbol);
        get_first_trade_time(&client, symbol, interval).await?
    } else {
        parse_date(start_date)?
    };
    
    let mut end_time = if end_date == "now" {
        Utc::now().timestamp_millis()
    } else {
        parse_date(end_date)?
    };
    
    // 对于日线数据，结束时间调整为前一天
    if interval == "1d" {
        end_time = end_time - 86400 * 1000; // 减去一天的毫秒数
    }
    
    // 创建HTTP客户端，设置超时
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    
    // 下载数据
    let klines = download_kline_data(&client, symbol, start_time, end_time, interval).await?;
    
    // 保存数据
    let filename = format!("{}_{}_{}.csv",
        symbol,
        DateTime::from_timestamp(start_time / 1000, 0)
            .unwrap()
            .format("%Y-%m-%d"),
        DateTime::from_timestamp(end_time / 1000, 0)
            .unwrap()
            .format("%Y-%m-%d")
    );
    save_to_csv(&klines, symbol)?;
    
    println!("数据已保存到 {}", filename);

    // 如果启用了renew模式
    if matches.get_flag("renew") {
        info!("启动持续获取模式...");
        
        // 保存最后时间戳到文件
        let state_file = format!("data/{}/.state", symbol);
        let last_time = klines.last().map(|k| k.time).unwrap_or(end_time);
        
        if let Err(e) = std::fs::write(&state_file, last_time.to_string()) {
            error!("无法保存状态文件: {}", e);
            return Err(e.into());
        }
        
        info!("最后时间戳 {} 已保存到 {}",
            DateTime::from_timestamp(last_time / 1000, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S"),
            state_file);

        // 调用renew_cryptodata程序
        let renew_process = std::process::Command::new("cargo")
            .arg("run")
            .arg("--bin")
            .arg("renew_cryptodata")
            .arg("--")
            .arg(symbol)
            .arg(interval)
            .spawn()?;

        info!("持续获取进程已启动，PID: {}", renew_process.id());
    }
    
    Ok(())
}