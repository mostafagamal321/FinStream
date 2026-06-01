import json
from pathlib import Path
from typing import Any, Dict, Optional


class JsonCheckpointStore:
    """
    Simple JSON checkpoint store for replay producers.

    Example:
    {
      "historical_market_replay": {
        "last_successful_row_index": 2093
      }
    }
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}

        try:
            with self.path.open("r", encoding="utf-8") as file:
                return json.load(file)
        except json.JSONDecodeError:
            return {}

    def save(self, state: Dict[str, Any]) -> None:
        temp_path = self.path.with_suffix(".tmp")

        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(state, file, indent=2, sort_keys=True)

        temp_path.replace(self.path)

    def get(self, namespace: str, key: str) -> Optional[Any]:
        state = self.load()
        return state.get(namespace, {}).get(key)

    def set(self, namespace: str, key: str, value: Any) -> None:
        state = self.load()
        state.setdefault(namespace, {})
        state[namespace][key] = value
        self.save(state)

    def reset(self, namespace: str, key: Optional[str] = None) -> None:
        state = self.load()

        if namespace not in state:
            return

        if key is None:
            state.pop(namespace, None)
        else:
            state[namespace].pop(key, None)

        self.save(state)