#!/usr/bin/env python3
# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

"""Generate a tiny ONNX model for testing the CockroachDB ONNX Runtime
integration. The model mimics a sentence-transformer's input/output signature:

  Inputs:
    - input_ids:      int64 [batch_size, sequence_length]
    - attention_mask:  int64 [batch_size, sequence_length]

  Outputs:
    - token_embeddings: float32 [batch_size, sequence_length, dims]

The model is intentionally tiny (~100KB) and NOT a real embedding model. It
exists solely to exercise the full CGo pipeline (init -> load -> infer -> read
output) without requiring a real model download.

Requirements:
    pip install torch onnx onnxscript

Usage:
    python generate_test_model.py            # writes test_model.onnx
    python generate_test_model.py out.onnx   # writes to custom path
"""

import sys

import torch
import torch.nn as nn


class TinyTransformer(nn.Module):
    """A minimal transformer-like model that produces token embeddings."""

    def __init__(self, vocab_size: int = 32, dims: int = 8, num_heads: int = 2,
                 num_layers: int = 1):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, dims)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=dims, nhead=num_heads, dim_feedforward=dims * 2,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer,
                                             num_layers=num_layers)

    def forward(self, input_ids: torch.Tensor,
                attention_mask: torch.Tensor) -> torch.Tensor:
        x = self.embedding(input_ids)
        # TransformerEncoder uses src_key_padding_mask where True = ignore.
        padding_mask = attention_mask == 0
        x = self.encoder(x, src_key_padding_mask=padding_mask)
        return x


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else "test_model.onnx"

    model = TinyTransformer(vocab_size=32, dims=8, num_heads=2, num_layers=1)
    model.eval()

    batch_size = 1
    seq_len = 4
    dummy_input_ids = torch.tensor([[1, 2, 3, 0]], dtype=torch.long)
    dummy_attention_mask = torch.tensor([[1, 1, 1, 0]], dtype=torch.long)

    torch.onnx.export(
        model,
        (dummy_input_ids, dummy_attention_mask),
        out_path,
        input_names=["input_ids", "attention_mask"],
        output_names=["token_embeddings"],
        dynamic_axes={
            "input_ids": {0: "batch_size", 1: "sequence_length"},
            "attention_mask": {0: "batch_size", 1: "sequence_length"},
            "token_embeddings": {0: "batch_size", 1: "sequence_length"},
        },
        opset_version=18,
    )
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
