from typing import List

import os


def load_categories(level: str) -> List[str]:
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(base_path, "data", "categories", f"{level}.txt")
    
    if not os.path.exists(data_path):
        return []
        
    with open(data_path, 'r', encoding='utf-8') as f:
        categories = [line.strip().strip('"') for line in f if line.strip()]
        
    return categories
