use tokio_tungstenite::connect_async;
use futures_util::stream::StreamExt;
use serde_json::Value;
use chrono::{DateTime, FixedOffset};

#[tokio::main]
async fn main() {
    // WebSocket URL for BTCUSDT ticker
    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";

    // 连接WebSocket
    let (ws_stream, _) = connect_async(url).await.expect("无法连接WebSocket");
    println!("成功连接到BTCUSDT交易数据流...");

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

                            // 计算USDT金额
                            let price_num: f64 = price.parse().unwrap();
                            let qty_num: f64 = qty.parse().unwrap();
                            let usdt_amount = price_num * qty_num;

                            // 格式化输出
                            println!(
                                "时间: {} | 价格: {} | 方向: {} | 数量: {}{:.5} BTC / {}{:.2} USDT",
                                local_time.format("%H:%M:%S%.3f"),
                                price,
                                if is_buyer_maker { "卖出" } else { "买入" },
                                if is_buyer_maker { "-" } else { "+" },
                                qty_num,
                                if is_buyer_maker { "-" } else { "+" },
                                usdt_amount
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
}
