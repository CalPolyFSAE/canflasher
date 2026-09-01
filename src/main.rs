#[actix_web::main]
async fn main() -> std::io::Result<()> {
    CanFlash::web::start_server("127.0.0.1:8080").await
}
