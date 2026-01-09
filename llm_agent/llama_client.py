import os
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

MODEL_NAME = os.getenv("LLAMA3_MODEL", "meta-llama/Meta-Llama-3-8B-Instruct")

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
    device_map="auto",
)

def generate_response(prompt: str, max_tokens: int = 512) -> str:
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(
        **inputs,
        max_new_tokens=max_tokens,
        do_sample=False,
        temperature=0.2,
    )
    text = tokenizer.decode(outputs[0], skip_special_tokens=True)
    # Strip prompt echo for instruct models if needed
    return text[len(prompt):].strip()
