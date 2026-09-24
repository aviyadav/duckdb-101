import asyncio
import os
from google.antigravity import Agent, LiteRTAgentConfig
from google.antigravity.hooks import policy

# UPDATE: Your prompt
PROMPT = "Build a command-line interface tool using the psutil and rich libraries that displays a live-updating terminal dashboard. It should show CPU usage, memory consumption, and a sorted table of the top 5 most memory-intensive processes. Save the script as 'monitor.py' and create a 'requirements.txt' file. Test that it works."

# UPDATE: Point to the locally imported LiteRT-LM model path
MODEL_PATH = os.path.expanduser("~/.litert-lm/models/gemma4-26b/model.litertlm")

# UPDATE: Give AGY-SDK a workspace to write files
WORKING_DIR = os.path.expanduser("~/agy-test")

os.makedirs(WORKING_DIR, exist_ok=True)
os.chdir(WORKING_DIR)

async def async_main():
  print(f"Using local LiteRT model: {MODEL_PATH}. Please wait for local inference to complete. This could take several minutes.")

  config = LiteRTAgentConfig(
     model_path=MODEL_PATH,
     workspaces=[WORKING_DIR],
     policies=[policy.allow_all()],
  ).lightweight()

  async with Agent(config) as agent:
     response = await agent.chat(PROMPT)
     async for token in response:
        print(token, end="", flush=True)

def main():
   asyncio.run(async_main())

if __name__ == "__main__":
   main()
