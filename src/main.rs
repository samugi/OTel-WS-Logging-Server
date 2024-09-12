use flate2::read::GzDecoder;
use std::io::Read;
use std::thread;
use websocket::{sync::Server, OwnedMessage};

use opentelemetry_proto::tonic::trace::v1::TracesData;
use opentelemetry_proto::tonic::logs::v1::LogsData;
use opentelemetry_proto::tonic::metrics::v1::MetricsData;


fn decode_proto_traces(content: &[u8]) -> Result<TracesData, prost::DecodeError> {
  let traces: Result<TracesData, _> = prost::Message::decode(content);
  traces
}

fn decode_proto_logs(content: &[u8]) -> Result<LogsData, prost::DecodeError> {
  let logs: Result<LogsData, _> = prost::Message::decode(content);
  logs
}

fn decode_proto_metrics(content: &[u8]) -> Result<MetricsData, prost::DecodeError> {
  let metrics: Result<MetricsData, _> = prost::Message::decode(content);
  metrics
}

fn proto_decode(decompressed_data: &Vec<u8>) {
  match decode_proto_logs(decompressed_data) {
    Ok(t) => {
      println!("Decoded Logs: {:?}", t);
    },
    Err(e) => {
      // println!("Failed to decode logs: {:?}", e);
    }
  }

  match decode_proto_traces(&decompressed_data) {
    Ok(t) => {
      println!("Decoded Traces: {:?}", t);
    },
    Err(e) => {
      // println!("Failed to decode traces: {:?}", e);
    }
  }

  match decode_proto_metrics(&decompressed_data) {
    Ok(t) => {
      println!("Decoded Metrics: {:?}", t);
    },
    Err(e) => {
      // println!("Failed to decode metrics: {:?}", e);
    }
  }
}

fn main() {
    // Start a WebSocket server that listens on port 8080
    let server = Server::bind("127.0.0.1:8080").expect("Failed to bind server");

    // For each incoming connection...
    for request in server.filter_map(Result::ok) {
        // Spawn a new thread to handle the connection
        thread::spawn(move || {
            // Accept the connection request
            let mut client = request.accept().expect("Failed to accept connection");

            // Listen for incoming messages
            for message in client.incoming_messages() {
                match message {
                    Ok(OwnedMessage::Text(text)) => {
                        // Print the received message
                        println!("Received text message: {}", text);
                    }
                    Ok(OwnedMessage::Binary(bin)) => {
                        // println!("Received binary message");
                        // println!("attempting gzip deflate");
                        let mut gz = GzDecoder::new(&bin[..]);
                        let mut decompressed_data = Vec::new();
                        match gz.read_to_end(&mut decompressed_data) {
                            Ok(_) => {
                              proto_decode(&decompressed_data);
                            },
                            Err(_) => {
                              println!("Apparently this data was not compressed");
                              proto_decode(&bin);
                            }
                        }
                    }
                    Ok(OwnedMessage::Close(_)) => {
                        println!("Client disconnected");
                        return;
                    }
                    Ok(OwnedMessage::Ping(_)) => {
                        println!("ping");
                        return;
                    }
                    Ok(OwnedMessage::Pong(_)) => {
                        println!("pong");
                        return;
                    }
                    Err(e) => {
                        println!("Error receiving message: {}", e);
                        return;
                    }
                }
            }
        });
    }
}
