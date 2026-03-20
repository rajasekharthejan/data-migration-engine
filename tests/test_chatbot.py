"""Tests for the AI lineage chatbot."""

import pytest

from src.chatbot.lineage_chatbot import ChatContext, LineageChatbot
from src.lineage.lineage_engine import LineageEngine


class TestChatbot:
    """Test suite for the lineage chatbot."""

    @pytest.fixture
    def engine(self):
        return LineageEngine()

    @pytest.fixture
    def chatbot(self, engine):
        return LineageChatbot(lineage_engine=engine)

    def test_intent_classification_backward(self, chatbot):
        assert chatbot._classify_intent("Where did premium_amount come from?") == "lineage_backward"

    def test_intent_classification_forward(self, chatbot):
        assert chatbot._classify_intent("What downstream systems use this field?") == "lineage_forward"

    def test_intent_classification_reconciliation(self, chatbot):
        assert chatbot._classify_intent("Did the reconciliation pass?") == "reconciliation"

    def test_intent_classification_transformation(self, chatbot):
        assert chatbot._classify_intent("How was the SSN field transformed?") == "transformation"

    def test_intent_classification_status(self, chatbot):
        assert chatbot._classify_intent("What's the job status?") == "status"

    def test_intent_classification_impact(self, chatbot):
        assert chatbot._classify_intent("What would break if I change this column?") == "impact"

    def test_mock_chat_response(self, chatbot):
        response = chatbot.chat("Where did the premium come from?")
        assert "answer" in response
        assert "intent" in response
        assert response["model"] == "mock"

    def test_entity_extraction_policy_id(self, chatbot):
        entities = chatbot._extract_entities("Show lineage for POL-123456")
        assert entities["record_id"] == "POL-123456"

    def test_entity_extraction_column(self, chatbot):
        entities = chatbot._extract_entities("What happened to premium_amount?")
        assert "premium_amount" in (entities.get("source_column", "") or entities.get("target_column", ""))

    def test_entity_extraction_table(self, chatbot):
        entities = chatbot._extract_entities("Show all records from claims table")
        assert "claims" in entities.get("source_table", "")

    def test_conversation_context(self):
        ctx = ChatContext(max_history=5)
        ctx.add_user_message("hello")
        ctx.add_assistant_message("hi")
        assert len(ctx.get_messages()) == 2

    def test_context_trimming(self):
        ctx = ChatContext(max_history=2)
        for i in range(10):
            ctx.add_user_message(f"msg {i}")
            ctx.add_assistant_message(f"reply {i}")
        assert len(ctx.get_messages()) <= 4  # max_history * 2
