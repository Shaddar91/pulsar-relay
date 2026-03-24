use crate::config::TelegramConfig;
use tracing::warn;

//Read bot token and chat_id from env vars named in config.
//Returns None if either is missing or empty.
fn resolve_credentials(config: &TelegramConfig) -> Option<(String, String)> {
    let token = std::env::var(&config.bot_token_env)
        .ok()
        .filter(|v| !v.is_empty())?;
    let chat_id = std::env::var(&config.chat_id_env)
        .ok()
        .filter(|v| !v.is_empty())?;
    Some((token, chat_id))
}

//POST a message to the Telegram Bot API.
//Logs a warning on failure, never panics.
async fn send_message(token: &str, chat_id: &str, text: &str) -> anyhow::Result<()> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
    let body = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    });

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("telegram API returned {}: {}", status, body);
    }

    Ok(())
}

//Top-level notify: checks enabled flag, resolves credentials, sends message.
//If anything is off (disabled, missing creds, API error), returns silently.
pub async fn notify(config: &TelegramConfig, text: &str) {
    if !config.enabled {
        return;
    }

    let (token, chat_id) = match resolve_credentials(config) {
        Some(creds) => creds,
        None => {
            warn!(
                bot_token_env = %config.bot_token_env,
                chat_id_env = %config.chat_id_env,
                "telegram credentials missing — skipping notification"
            );
            return;
        }
    };

    if let Err(e) = send_message(&token, &chat_id, text).await {
        warn!(error = %e, "failed to send telegram notification");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn resolve_credentials_returns_none_when_env_vars_not_set() {
        let config = TelegramConfig {
            enabled: true,
            bot_token_env: "_TEST_TG_TOKEN_MISSING".to_string(),
            chat_id_env: "_TEST_TG_CHAT_MISSING".to_string(),
            ..Default::default()
        };
        std::env::remove_var("_TEST_TG_TOKEN_MISSING");
        std::env::remove_var("_TEST_TG_CHAT_MISSING");
        assert!(resolve_credentials(&config).is_none());
    }

    #[test]
    #[serial]
    fn resolve_credentials_returns_none_when_token_empty() {
        std::env::set_var("_TEST_TG_TOKEN_EMPTY", "");
        std::env::set_var("_TEST_TG_CHAT_EMPTY", "12345");
        let config = TelegramConfig {
            enabled: true,
            bot_token_env: "_TEST_TG_TOKEN_EMPTY".to_string(),
            chat_id_env: "_TEST_TG_CHAT_EMPTY".to_string(),
            ..Default::default()
        };
        assert!(resolve_credentials(&config).is_none());
        std::env::remove_var("_TEST_TG_TOKEN_EMPTY");
        std::env::remove_var("_TEST_TG_CHAT_EMPTY");
    }

    #[test]
    #[serial]
    fn resolve_credentials_returns_some_when_both_set() {
        std::env::set_var("_TEST_TG_TOKEN_OK", "bot123:ABC");
        std::env::set_var("_TEST_TG_CHAT_OK", "999");
        let config = TelegramConfig {
            enabled: true,
            bot_token_env: "_TEST_TG_TOKEN_OK".to_string(),
            chat_id_env: "_TEST_TG_CHAT_OK".to_string(),
            ..Default::default()
        };
        let creds = resolve_credentials(&config).unwrap();
        assert_eq!(creds.0, "bot123:ABC");
        assert_eq!(creds.1, "999");
        std::env::remove_var("_TEST_TG_TOKEN_OK");
        std::env::remove_var("_TEST_TG_CHAT_OK");
    }

    #[test]
    fn telegram_config_defaults_all_false() {
        let config = TelegramConfig::default();
        assert!(!config.enabled);
        assert!(!config.notify_plan_completed);
        assert!(!config.notify_plan_failed);
        assert!(!config.notify_component_completed);
        assert_eq!(config.bot_token_env, "TELEGRAM_BOT_TOKEN");
        assert_eq!(config.chat_id_env, "TELEGRAM_OWNER_ID");
    }
}
