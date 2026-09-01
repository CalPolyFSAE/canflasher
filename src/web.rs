use std::sync::Mutex;

use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};
use can_hal::CanId;
use can_hal_kvaser::{Classic, KvaserChannel};

use crate::Manager;

const MAX_BINARY_SIZE: usize = 16 * 1024 * 1024;
const SELF_CAN_ID: CanId = CanId::Standard(0x7FF);

type FlashManager = Manager<KvaserChannel<Classic>>;

#[post("/flash/{can_id}/{window_size}")]
async fn flash_binary(
    manager: web::Data<Mutex<FlashManager>>,
    path: web::Path<(u16, usize)>,
    binary: web::Bytes,
) -> impl Responder {
    let (raw_can_id, window_size) = path.into_inner();
    let Some(can_id) = CanId::new_standard(raw_can_id) else {
        return HttpResponse::BadRequest().body("CAN ID must be between 0 and 2047");
    };

    let result = web::block(move || {
        let mut manager = manager
            .lock()
            .map_err(|_| "flash manager lock is poisoned".to_owned())?;
        manager
            .upload_binary(SELF_CAN_ID, can_id, &binary, window_size)
            .map_err(|error| error.to_string())
    })
    .await;

    match result {
        Ok(Ok(())) => HttpResponse::Ok().body("flash completed"),
        Ok(Err(error)) => HttpResponse::BadRequest().body(error),
        Err(error) => HttpResponse::InternalServerError().body(error.to_string()),
    }
}

pub async fn start_server(bind_address: &str) -> std::io::Result<()> {
    let manager = FlashManager::new_kvaser().map_err(|error| {
        std::io::Error::other(format!("failed to open Kvaser channel: {error}"))
    })?;
    let manager = web::Data::new(Mutex::new(manager));

    HttpServer::new(move || {
        App::new()
            .app_data(manager.clone())
            .app_data(web::PayloadConfig::new(MAX_BINARY_SIZE))
            .service(flash_binary)
    })
    .bind(bind_address)?
    .run()
    .await
}
