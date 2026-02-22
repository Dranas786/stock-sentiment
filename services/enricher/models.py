# services/enricher/models.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import torch
from transformers import pipeline


DEFAULT_FINBERT_MODEL = "ProsusAI/finbert"
DEFAULT_EMOTION_MODEL = "cardiffnlp/twitter-roberta-base-emotion"
DEFAULT_ZEROSHOT_MODEL = "facebook/bart-large-mnli"

STANCE_LABELS = ["bullish", "bearish", "news", "question", "meme", "neutral"]


@dataclass(frozen=True)
class FinSentiment:
    pos: float
    neg: float
    neutral: float
    score: float  # [-1, 1] (pos - neg)


@dataclass(frozen=True)
class Stance:
    label: str
    confidence: float
    probs: Dict[str, float]


class HFModels:
    """
    Loads HF pipelines once and provides batched inference for:
      - finance sentiment (FinBERT)
      - emotion (twitter-roberta emotion)
      - stance/intent (zero-shot)

    Notes:
      - By default uses CPU. If you later add GPU, set device=0.
      - Uses truncation to keep inference stable.
      - Batching is critical for throughput.
    """

    def __init__(
        self,
        finbert_model: str = DEFAULT_FINBERT_MODEL,
        emotion_model: str = DEFAULT_EMOTION_MODEL,
        zeroshot_model: str = DEFAULT_ZEROSHOT_MODEL,
        device: Optional[int] = None,
        batch_size: int = 16,
        max_length: int = 256,
    ) -> None:
        self.batch_size = batch_size
        self.max_length = max_length

        # Device: None -> auto pick CPU
        if device is None:
            device = 0 if torch.cuda.is_available() else -1
        self.device = device

        # Finance sentiment
        self._fin = pipeline(
            task="text-classification",
            model=finbert_model,
            tokenizer=finbert_model,
            return_all_scores=True,
            device=self.device,
            truncation=True,
            max_length=self.max_length,
        )

        # Emotion classifier
        self._emo = pipeline(
            task="text-classification",
            model=emotion_model,
            tokenizer=emotion_model,
            return_all_scores=True,
            device=self.device,
            truncation=True,
            max_length=self.max_length,
        )

        # Zero-shot stance classifier
        self._zs = pipeline(
            task="zero-shot-classification",
            model=zeroshot_model,
            tokenizer=zeroshot_model,
            device=self.device,
        )

    def finbert_batch(self, texts: List[str]) -> List[FinSentiment]:
        """
        Returns finance sentiment distribution + continuous score.

        FinBERT labels are typically: 'positive', 'negative', 'neutral'
        We compute score = pos - neg in [-1, 1].
        """
        out: List[FinSentiment] = []
        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i : i + self.batch_size]
            preds = self._fin(chunk)  # list[list[{label, score}]]

            for scores in preds:
                d = {s["label"].lower(): float(s["score"]) for s in scores}
                pos = d.get("positive", 0.0)
                neg = d.get("negative", 0.0)
                neu = d.get("neutral", 0.0)
                out.append(
                    FinSentiment(
                        pos=pos,
                        neg=neg,
                        neutral=neu,
                        score=max(-1.0, min(1.0, pos - neg)),
                    )
                )
        return out

    def emotion_batch(self, texts: List[str]) -> List[Dict[str, float]]:
        """
        Returns emotion probability distribution.

        Model label set depends on the model; cardiffnlp emotion commonly uses:
          anger, joy, optimism, sadness
        Some variants include fear, surprise, etc.
        We pass through whatever labels the model provides.
        """
        out: List[Dict[str, float]] = []
        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i : i + self.batch_size]
            preds = self._emo(chunk)  # list[list[{label, score}]]

            for scores in preds:
                d = {s["label"].lower(): float(s["score"]) for s in scores}
                out.append(d)
        return out

    def stance_batch(
        self,
        texts: List[str],
        labels: Optional[List[str]] = None,
    ) -> List[Stance]:
        """
        Zero-shot classify stance/intent.

        We default to STANCE_LABELS.
        We use multi_label=True because a post can be both "news" and "bullish-ish"
        but we will still pick the top label for a single stance value.
        """
        if labels is None:
            labels = STANCE_LABELS

        out: List[Stance] = []
        for i in range(0, len(texts), self.batch_size):
            chunk = texts[i : i + self.batch_size]

            preds = self._zs(
                chunk,
                candidate_labels=labels,
                multi_label=True,
            )
            # preds can be dict (single) or list[dict] (batch)
            if isinstance(preds, dict):
                preds_list = [preds]
            else:
                preds_list = preds

            for p in preds_list:
                # labels are ordered high->low, scores aligned
                lbls = [str(x).lower() for x in p["labels"]]
                scs = [float(x) for x in p["scores"]]
                probs = dict(zip(lbls, scs))

                top_label = lbls[0] if lbls else "neutral"
                top_conf = scs[0] if scs else 0.0
                out.append(Stance(label=top_label, confidence=top_conf, probs=probs))

        return out


def normalize_text(text: str, max_chars: int = 5000) -> str:
    """
    Light normalization to keep inference stable and avoid huge payloads.
    """
    if not text:
        return ""
    t = " ".join(text.split())
    if len(t) > max_chars:
        t = t[:max_chars]
    return t


def make_conviction_score(stance: Stance, fin: FinSentiment) -> float:
    """
    A simple, model-driven conviction proxy (no keyword lists).
    Higher when:
      - stance is bullish/bearish with high confidence
      - FinBERT is strongly pos or strongly neg

    Output: [0, 1]
    """
    stance_strength = 0.0
    if stance.label in ("bullish", "bearish"):
        stance_strength = stance.confidence

    sent_strength = abs(fin.score)  # [0, 1]

    # Combine with weights (explainable and stable)
    raw = 0.65 * stance_strength + 0.35 * sent_strength
    return max(0.0, min(1.0, raw))
