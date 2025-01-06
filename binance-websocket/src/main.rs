mod clickhouse_connect;

use clickhouse_connect::test_connection;

#[tokio::main]
async fn main() {
    // 测试数据库连接
    match test_connection().await {
        Ok(_) => println!("数据库连接测试成功"),
        Err(e) => {
            eprintln!("数据库连接失败: {}", e);
            eprintln!("详细错误信息: {:?}", e);
        }
    }
}
