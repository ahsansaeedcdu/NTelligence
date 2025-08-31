from typing import List, Literal, Any, Optional
from pydantic import BaseModel, conint, field_validator, model_validator

class NarratorOutput(BaseModel):
    summary: str

class Measure(BaseModel):
    name: str
    agg: Literal['sum','avg','min','max','count','count_distinct']
    column: str

class Filter(BaseModel):
    column: str
    op: Literal['=','!=','<','>','<=','>=','IN','BETWEEN','LIKE','IS NULL','IS NOT NULL']
    value: Optional[Any] = None  # None allowed for IS NULL / IS NOT NULL

class Order(BaseModel):
    expr: Optional[str] = None
    dir: Literal['asc','desc'] = 'desc'

    @field_validator('dir', mode='after')
    @classmethod
    def normalize_dir(cls, v: str) -> str:
        return v.lower()

class QueryPlan(BaseModel):
    table: Literal['employee','action','perf','join_emp_perf','join_emp_action']
    intent: Literal['aggregate','select','topk'] = 'aggregate'
    dimensions: List[str] = []
    measures: List[Measure]
    filters: List[Filter] = []
    order_by: List[Order] = []
     # keep strict bounds if you want
    limit: int = 100

    @field_validator("limit", mode="before")
    @classmethod
    def _coerce_limit(cls, v: Any) -> int:
        # turn 0/None/"" into default; clamp extremes
        try:
            if v in (None, "", 0, "0", False, [], {}):
                return 100
            i = int(v)
        except Exception:
            return 100
        if i < 1:
            return 100
        if i > 1000:
            return 1000
        return i

    @model_validator(mode='after')
    def normalize(self):
        # Ensure at least one measure; helps downstream defaults
        if not self.measures:
            raise ValueError("At least one measure is required")

        # Fill empty order_by with first measure alias, DESC
        if not self.order_by:
            self.order_by = [Order(expr=self.measures[0].name, dir='desc')]

        # If any order item is missing expr, point it at first measure alias
        first_alias = self.measures[0].name
        self.order_by = [Order(expr=o.expr or first_alias, dir=o.dir) for o in self.order_by]
        return self
