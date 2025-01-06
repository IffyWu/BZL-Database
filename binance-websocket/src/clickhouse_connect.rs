use clickhouse::Client;
use std::error::Error;

// 数据库连接配置
const DB_HOST: &str = "45.116.76.87";
const DB_PORT: u16 = 18123;
const DB_USER: &str = "default";
const DB_NAME: &str = "default";

// 创建数据库客户端
pub fn create_client() -> Client {
    // 构建连接字符串
    let database_url = format!(
        "http://{}:{}?user={}&database={}",
        DB_HOST, DB_PORT, DB_USER, DB_NAME
    );
    
    // 创建客户端
    Client::default()
        .with_url(database_url)
        .with_option("connect_timeout", "10")
}

// 测试数据库连接
pub async fn test_connection() -> Result<(), Box<dyn Error>> {
    let client = create_client();
    
    // 执行简单查询测试连接
    let result: Vec<u8> = client.query("SELECT 1").fetch_all().await?;
    
    if result.len() == 1 {
        println!("数据库连接测试成功");
        Ok(())
    } else {
        Err("数据库连接测试失败".into())
    }
}