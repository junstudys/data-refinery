import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


@dataclass
class PipelineState:
    current_step: int = 0
    completed_files: dict = None
    failed_files: dict = None
    last_run: str = ""

    def __post_init__(self):
        self.completed_files = self.completed_files or {}
        self.failed_files = self.failed_files or {}


class StateManager:
    def __init__(self, state_file: str = ".pipeline_state.json"):
        self.state_file = Path(state_file)

    def save(self, state: PipelineState) -> None:
        state.last_run = datetime.now().isoformat()
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(asdict(state), f, indent=2, ensure_ascii=False)

    def load(self) -> PipelineState:
        if self.state_file.exists():
            with open(self.state_file, encoding="utf-8") as f:
                return PipelineState(**json.load(f))
        return PipelineState()

    def mark_completed(self, step: int, file: str) -> None:
        state = self.load()
        if step not in state.completed_files:
            state.completed_files[step] = []
        state.completed_files[step].append(file)
        self.save(state)
