from pydantic import BaseModel, Field


class TopicCreateRequest(BaseModel):
    name: str = Field(min_length=1)


class PublishRequest(BaseModel):
    body: str
    headers: dict[str, str] = Field(default_factory=dict)


class PublishResponse(BaseModel):
    message_id: str
    delivered_to: int


class SubscriptionCreateRequest(BaseModel):
    destination: str
    subscription_id: str | None = None


class SubscriptionCreateResponse(BaseModel):
    subscription_id: str
    destination: str


class PollResponse(BaseModel):
    message_id: str
    destination: str
    body: str
    headers: dict[str, str]
