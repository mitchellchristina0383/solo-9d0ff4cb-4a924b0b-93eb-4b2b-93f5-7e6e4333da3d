from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from .scheduler import StepType


class StepIn(BaseModel):
    type: StepType
    workspace: Optional[str] = Field(default=None, description="required for acquire/release")
    duration_ms: int = Field(default=0, ge=0, description="simulated compile time")


class BuildIn(BaseModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    base_priority: int = Field(ge=0, description="larger value = higher priority")
    steps: list[StepIn] = Field(min_length=1)


class WorkspaceIn(BaseModel):
    id: str = Field(min_length=1)


class ResetIn(BaseModel):
    workspace_ids: list[str] = Field(min_length=1)


class AdvanceIn(BaseModel):
    steps: int = Field(default=1, ge=1, le=10_000)


class ErrorOut(BaseModel):
    detail: str
