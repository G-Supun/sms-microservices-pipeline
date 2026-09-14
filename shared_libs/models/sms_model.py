from typing import Optional, List
from pydantic import BaseModel, Field

class SMSClassificationRecord(BaseModel):
    """
    Represents a single classified SMS record matching
    the PostgreSQL sms_classifications table schema.
    """
    mobile: Optional[str] = Field(default=None, max_length=20)
    message: str = Field(..., min_length=1)
    is_transactional: bool = Field(...)

    class Config:
        from_attributes = True

class SingleSMSRequest(BaseModel):
    """
    Schema for single text classification requests via JSON.
    """
    mobile: Optional[str] = Field(default=None, max_length=20)
    message: str = Field(..., min_length=1, description="Raw SMS text to classify")

class BatchSMSRequest(BaseModel):
    """
    Schema for batch JSON classification requests.
    """
    records: List[SingleSMSRequest]