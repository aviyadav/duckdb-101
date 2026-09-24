import asyncio
import os
from google.antigravity import Agent, LiteRTAgentConfig
from google.antigravity.hooks import policy

# UPDATE: Point to the locally downloaded model from the previous step (litert-lm import ...)
MODEL_PATH = os.path.expanduser("~/.litert-lm/models/gemma4-26b/model.litertlm")

async def async_main():
   print(f"Using local LiteRT model: {MODEL_PATH}. Please wait for local inference to complete. This could take several minutes.")

   config = LiteRTAgentConfig(model_path=MODEL_PATH).lightweight()
   async with Agent(config) as agent:
      response = await agent.chat("What files are in the current directory?")
      async for token in response:
         print(token, end="", flush=True)

def main():
   asyncio.run(async_main())

if __name__ == "__main__":
   main()
