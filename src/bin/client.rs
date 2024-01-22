#![allow(unused)]

use clap::builder::Str;
use clap::Parser;
use std::error::Error;
use std::io;
use std::net::Shutdown;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::stream;

#[derive(Parser, Debug)]
#[command(version, about = "")]
pub struct Args {
    #[arg(short, long)]
    pub address: String,
    #[arg(short, long)]
    pub port: String,
}

async fn read_the_clients_message() -> String {
    let mut message = String::new();
    match io::stdin().read_line(&mut message) {
        Ok(_) => return message,
        Err(e) => {
            panic!("{}", e);
        }
    }
}

#[tokio::main]
async fn main() {
    let mut is_an_error = false;
    let args = Args::parse();

    let mut servers_address = format!("{}:{}", args.address, args.port);

    match TcpStream::connect(servers_address.clone()).await {
        Err(e) => {
            println!("Failed to connect to the kafka server: {}", e);
        }
        Ok(mut stream) => {
            println!(
                "Successfully connected to the kafka server to port {}",
                servers_address
            );
            let mut message = read_the_clients_message().await;
            message = format!("[{}]", message);
            let mut method_and_topic = kafka_lib::parser::parser_json(message);
            let mut method_and_topic = match method_and_topic {
                Err(e) => {
                    panic!("Cant parse json:{}", e);
                    is_an_error = true;
                }
                Ok(x) => x,
            };
            let mut method = method_and_topic.0;
            let mut topic = method_and_topic.1;
            method.pop();
            method = method[1..].to_string();
            topic.pop();
            topic = topic[1..].to_string();

            if (method == "publish".to_string() && is_an_error == false) {
                let mut request = format!("publish`{topic}#");
                stream.write_all(request.as_bytes()).await.unwrap();
                loop {
                    let mut message = read_the_clients_message().await;
                    let mut request = format!("{message}#");
                    stream.write_all(request.as_bytes()).await.unwrap();

                    let mut buffer = vec![];
                    let mut response = BufReader::new(&mut stream)
                        .read_until(b'#', &mut buffer)
                        .await
                        .unwrap();
                    let mut response = String::from_utf8(buffer).unwrap();
                    response.pop();

                    if (response == "error") {
                        stream.shutdown();
                        println!("Termineted due to not valid json");
                        break;
                    }
                }
            } else if (method == "subscribe".to_string() && is_an_error == false) {
                let mut request = format!("subscribe`{topic}#");
                stream.write_all(request.as_bytes()).await.unwrap();

                loop {
                    let mut buffer = vec![];
                    let mut response = BufReader::new(&mut stream)
                        .read_until(b'#', &mut buffer)
                        .await
                        .unwrap();
                    let mut response = String::from_utf8(buffer).unwrap();
                    response.pop();
                    print!("{}", response);
                }
            }
        }
    }
    println!("Terminated");
}
