use crate::config::AgentConfig;
use std::process::Command;
use tracing::info;

pub struct AgentSpawner {
    spawn_script: String,
    default_model: String,
    timeout_secs: u64,
}

impl AgentSpawner {
    pub fn new(config: &AgentConfig) -> Self {
        Self {
            spawn_script: config.spawn_script.to_string_lossy().to_string(),
            default_model: config.default_model.clone(),
            timeout_secs: config.component_timeout_secs,
        }
    }

    //spawn an agent to work on a specific component of a task
    //the agent reads the full task file, identifies the current component,
    //works on it, and writes results back to the file
    pub async fn spawn(
        &self,
        agent_name: &str,
        task_file_path: &str,
        component_index: usize,
    ) -> anyhow::Result<String> {
        let prompt = format!(
            "You are working on a sequential multi-component task managed by pulsar-relay scheduler.\n\
             \n\
             TASK FILE: {}\n\
             CURRENT COMPONENT: Component {}\n\
             \n\
             Instructions:\n\
             1. Read the full task file to understand the overall goal and previous components' results\n\
             2. Focus ONLY on Component {} — do not work on other components\n\
             3. Complete the work described in Component {}\n\
             4. Your output will be captured and written back to the task file as the result\n\
             5. If you need information from previous components, check their **Result:** sections\n\
             \n\
             **Scheduler:** pulsar-relay\n\
             **Component:** {}\n\
             \n\
             Read the task file and begin working on Component {}.",
            task_file_path,
            component_index,
            component_index,
            component_index,
            component_index,
            component_index,
        );

        info!(
            agent = %agent_name,
            task_file = %task_file_path,
            component = component_index,
            "spawning agent"
        );

        //spawn via spawn-agent-v2.sh
        let output = tokio::task::spawn_blocking({
            let script = self.spawn_script.clone();
            let model = self.default_model.clone();
            let agent = agent_name.to_string();
            let prompt = prompt.clone();
            let _timeout = self.timeout_secs;

            move || {
                let result = Command::new("bash")
                    .arg(&script)
                    .arg("--agent")
                    .arg(&agent)
                    .arg("--model")
                    .arg(&model)
                    .arg("--prompt")
                    .arg(&prompt)
                    .env("PULSAR_RELAY_TASK", "true")
                    .output();

                match result {
                    Ok(output) => {
                        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

                        if output.status.success() {
                            Ok(stdout)
                        } else {
                            Err(anyhow::anyhow!(
                                "agent exited with {}: {}",
                                output.status,
                                stderr
                            ))
                        }
                    }
                    Err(e) => Err(anyhow::anyhow!("failed to spawn agent: {}", e)),
                }
            }
        }).await??;

        info!(
            agent = %agent_name,
            component = component_index,
            output_len = output.len(),
            "agent completed"
        );

        Ok(output)
    }
}
