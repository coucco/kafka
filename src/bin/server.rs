#![allow(unused)]

use clap::builder::Str;
use clap::Parser;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};

#[derive(Parser, Debug)]
#[command(version, about = "")]
pub struct Args {
    #[arg(short, long)]
    pub address: String,
    #[arg(short, long)]
    pub port: String,
}

async fn handle_client(
    mut stream: TcpStream,
    mut topic_senders: Arc<Mutex<HashMap<String, broadcast::Sender<Arc<String>>>>>,
) {
    let mut clients_address = stream.local_addr().unwrap().to_string().clone();
    let mut buffer = vec![];
    let mut request = BufReader::new(&mut stream)
        .read_until(b'#', &mut buffer)
        .await
        .unwrap();
    let mut request = String::from_utf8(buffer).unwrap();

    if request.clone().starts_with("publish`") {
        request = request.replace("publish`", "").to_string();
        request.pop();
        let topic = request;

        println!(
            "For topic:{} connected publisher with ip {}",
            topic,
            stream.peer_addr().unwrap()
        );

        if topic_senders.lock().await.contains_key(&topic) == false {
            let (tx, _) = broadcast::channel(100000);
            topic_senders.lock().await.insert(topic.clone(), tx.clone());
        }

        let mut tx_cloned = topic_senders.lock().await.get(&topic).unwrap().clone();
        tokio::spawn(async move {
            loop {
                let mut buffer = vec![];
                let mut response = BufReader::new(&mut stream)
                    .read_until(b'#', &mut buffer)
                    .await
                    .unwrap();
                let mut response = String::from_utf8(buffer).unwrap();
                response.pop();
                let json_data = serde_json::from_str::<serde_json::Value>(
                    &format!("[{}]", response.clone()).to_string(),
                );
                match json_data {
                    Err(e) => {
                        let mut message = "error";
                        let mut request = format!("{message}#");
                        stream.write_all(request.as_bytes()).await.unwrap();
                        println!(
                            "Failed to parse message from publisher: {}, {}",
                            clients_address, e
                        );
                        stream.shutdown();
                    }
                    Ok(_) => {
                        let mut message = "ok";
                        let mut request = format!("{message}#");
                        stream.write_all(request.as_bytes()).await.unwrap();
                        tx_cloned.send(Arc::new(response.to_string()));
                    }
                }
            }
        });
    } else if request.clone().starts_with("subscribe`") {
        request = request.replace("subscribe`", "").to_string();
        request.pop();
        let topic = request;

        println!(
            "For topic: {} connected subscriber with ip {}",
            topic,
            stream.peer_addr().unwrap()
        );

        if topic_senders.lock().await.contains_key(&topic) == false {
            let (tx, _) = broadcast::channel(100000);
            topic_senders.lock().await.insert(topic.clone(), tx.clone());
        }
        let mut reciever = topic_senders
            .lock()
            .await
            .get(&topic)
            .unwrap()
            .clone()
            .subscribe();
        tokio::spawn(async move {
            loop {
                let mut message = reciever.recv().await.unwrap();
                let mut message = (format!("{message}#")).to_string();
                if (message != "".to_string()) {
                    stream.write_all(message.as_bytes()).await.unwrap();
                }
            }
        });
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut servers_address = format!("{}:{}", args.address, args.port);

    let listener = TcpListener::bind(servers_address.clone()).await.unwrap();
    println!("Kafka server has been started at {}", servers_address);

    let mut topic_senders: Arc<Mutex<HashMap<String, broadcast::Sender<Arc<String>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    loop {
        match listener.accept().await {
            Err(e) => {
                println!("Error: {}", e)
            }
            Ok((mut stream, _)) => {
                let mut topic_senders_clone: Arc<
                    Mutex<HashMap<String, broadcast::Sender<Arc<String>>>>,
                > = topic_senders.clone();
                handle_client(stream, topic_senders_clone).await;
            }
        }
    }
}
