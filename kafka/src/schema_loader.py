import json
from pathlib import Path

# Путь к директории со схемами
SCHEMA_DIR = Path(__file__).parent.parent / "schema"

def load_schema(schema_name: str) -> str:
    schema_path = SCHEMA_DIR / f"{schema_name}.avsc"
    
    if not schema_path.exists():
        raise FileNotFoundError(f"Схема не найдена: {schema_path}")
    
    with open(schema_path, 'r') as f:
        return f.read()

def get_order_event_schema() -> str:
    """Возвращает основную схему OrderEvent"""
    return load_schema("event")

def get_transformed_event_schema() -> str:
    """Возвращает схему трансформированного события"""
    return load_schema("transformed_event")

def get_aggregated_event_schema() -> str:
    """Возвращает схему агрегированного события"""
    return load_schema("aggregated_event")

def get_windowed_event_schema() -> str:
    """Возвращает схему оконного события"""
    return load_schema("windowed_event")
