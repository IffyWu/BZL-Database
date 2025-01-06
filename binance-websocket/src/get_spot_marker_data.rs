use tokio_tungstenite::connect_async;
use futures_util::stream::StreamExt;
use serde_json::Value;
use chrono::{DateTime, FixedOffset};

// 获取现货交易撮合数据
pub async fn get_spot_marker_data(symbol: &str) -> Result<(), Box<dyn std::error::Error>> {
    // 构建WebSocket URL
    let url = format!("wss://stream.binance.com:9443/ws/{}@trade", symbol.to_lowercase());

    // 连接WebSocket
    let (ws_stream, _) = connect_async(&url).await?;
    println!("成功连接到{}交易数据流...", symbol);

    // 处理接收到的消息
    let (_, mut read) = ws_stream.split();
    while let Some(msg) = read.next().await {
        match msg {
            Ok(msg) => {
                if let Ok(text) = msg.into_text() {
                    if let Ok(data) = serde_json::from_str::<Value>(&text) {
                        // 解析数据
                        if let (Some(price), Some(qty), Some(time), Some(is_buyer_maker)) = (
                            data["p"].as_str(),
                            data["q"].as_str(),
                            data["T"].as_u64(),
                            data["m"].as_bool(),
                        ) {
                            // 转换时间为东八区
                            let utc_time = DateTime::from_timestamp_millis(time as i64).unwrap();
                            let east8 = FixedOffset::east_opt(8 * 3600).unwrap();
                            let local_time = utc_time.with_timezone(&east8);

                            // 计算交易金额
                            let price_num: f64 = price.parse().unwrap();
                            let qty_num: f64 = qty.parse().unwrap();
                            let amount = price_num * qty_num;

                            // 格式化输出
                            println!(
                                "时间: {} | 价格: {} | 方向: {} | 数量: {}{:.5} {} / {}{:.2} USDT",
                                local_time.format("%H:%M:%S%.3f"),
                                price,
                                if is_buyer_maker { "卖出" } else { "买入" },
                                if is_buyer_maker { "-" } else { "+" },
                                qty_num,
                                symbol,
                                if is_buyer_maker { "-" } else { "+" },
                                amount
                            );
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("WebSocket错误: {}", e);
                break;
            }
        }
    }

    Ok(())
}