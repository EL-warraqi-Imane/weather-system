"""
Architecture exacte du modèle Transformer
DOIT ÊTRE IDENTIQUE à celle utilisée pendant l'entraînement
"""

import torch
import torch.nn as nn
import numpy as np

# Définir exactement la même architecture que ton entraînement
class RNNModel(nn.Module):
    def __init__(self, mode, in_dim, out_dim):
        super().__init__()
        self.rnn = nn.GRU(in_dim, 128, 2, batch_first=True)
        self.fc = nn.Linear(128, out_dim)
    def forward(self, x):
        out, _ = self.rnn(x)
        return self.fc(out[:, -1, :])