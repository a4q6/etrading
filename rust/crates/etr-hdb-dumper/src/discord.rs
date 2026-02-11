use tracing::{error, info};

/// Send a message to Discord via webhook URL.
/// Splits long messages into 2000-char chunks (Discord limit).
pub async fn send_discord_webhook(
    message: &str,
    username: &str,
    webhook_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let chunks = split_message(message, 2000);

    for chunk in &chunks {
        let body = serde_json::json!({
            "content": chunk,
            "username": username,
        });

        let resp = client.post(webhook_url).json(&body).send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            error!(status = %status, body = %text, "Discord webhook failed");
        } else {
            info!("Discord notification sent ({} chars)", chunk.len());
        }

        // Small delay between chunks to avoid rate limiting
        if chunks.len() > 1 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
    Ok(())
}

fn split_message(msg: &str, max_len: usize) -> Vec<String> {
    if msg.len() <= max_len {
        return vec![msg.to_string()];
    }
    let mut chunks = Vec::new();
    let mut remaining = msg;
    while !remaining.is_empty() {
        let end = std::cmp::min(max_len, remaining.len());
        // Try to split at a newline boundary
        let split_at = if end < remaining.len() {
            remaining[..end]
                .rfind('\n')
                .map(|i| i + 1)
                .unwrap_or(end)
        } else {
            end
        };
        chunks.push(remaining[..split_at].to_string());
        remaining = &remaining[split_at..];
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_message_short() {
        let chunks = split_message("hello", 2000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "hello");
    }

    #[test]
    fn test_split_message_long() {
        let msg = "a\n".repeat(1500);
        let chunks = split_message(&msg, 2000);
        assert!(chunks.len() >= 1);
        for chunk in &chunks {
            assert!(chunk.len() <= 2000);
        }
    }
}
