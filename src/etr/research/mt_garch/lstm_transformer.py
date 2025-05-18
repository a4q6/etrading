# transformer_lstm_torch.py
import torch
import torch.nn as nn
import torch.nn.functional as F
import math
import pandas as pd
import numpy as np
from typing import Tuple, Dict, List, Optional
from torch.utils.data import Dataset, DataLoader


class PositionalEncoding(nn.Module):
    def __init__(self, embed_dim, max_len=5000):
        super().__init__()
        pe = torch.zeros(max_len, embed_dim)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, embed_dim, 2).float() * (-np.log(10000.0) / embed_dim))

        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)  # shape: (1, max_len, embed_dim)
        self.register_buffer('pe', pe)

    def forward(self, x):
        # x: (batch_size, seq_len, embed_dim)
        x = x + self.pe[:, :x.size(1), :]
        return x


class LSTMTransformer(nn.Module):
    def __init__(self,
                 total_input_dim: int,
                 cat_index: Dict[int, int],  # {index_in_input: num_categories}
                 seq_len: int,
                 lstm_dim: int = 16,
                 num_heads: int = 4,
                 ff_dim: int = 8,
                 dropout: float = 0.1,
                 activation=nn.Identity(),  # nn.Sigmoid() for classification
                 ):
        super().__init__()

        self.cat_indices = sorted(cat_index.keys())
        self.num_categories = [cat_index[i] for i in self.cat_indices]
        self.seq_len = seq_len
        self.total_input_dim = total_input_dim
        self.numeric_indices = [i for i in range(total_input_dim) if i not in self.cat_indices]

        # Create embeddings based on sqrt(num_categories)
        self.embeddings = nn.ModuleList()
        self.embedding_dims = []
        for n_cat in self.num_categories:
            emb_dim = math.ceil(math.sqrt(n_cat))
            self.embeddings.append(nn.Embedding(n_cat, emb_dim))
            self.embedding_dims.append(emb_dim)

        self.total_emb_dim = sum(self.embedding_dims)
        self.effective_input_dim = len(self.numeric_indices) + self.total_emb_dim

        self.lstm = nn.LSTM(input_size=self.effective_input_dim,
                            hidden_size=lstm_dim,
                            batch_first=True)

        self.attn = nn.MultiheadAttention(embed_dim=lstm_dim,
                                          num_heads=num_heads,
                                          batch_first=True)

        self.norm1 = nn.LayerNorm(lstm_dim)
        self.ffn = nn.Sequential(
            nn.Linear(lstm_dim, ff_dim),
            nn.ReLU(),
            nn.Linear(ff_dim, lstm_dim)
        )
        self.norm2 = nn.LayerNorm(lstm_dim)

        self.global_pool = nn.AdaptiveAvgPool1d(1)
        self.fc1 = nn.Linear(lstm_dim, 8)
        self.dropout = nn.Dropout(dropout)
        self.fc2 = nn.Linear(8, 1)
        self.activation = activation  # nn.Identity for regression

    def forward(self, x: torch.Tensor):
        # x: (batch, seq_len, total_input_dim)
        x_cat = [x[:, :, idx].long() for idx in self.cat_indices]  # (batch, seq_len)
        x_num = x[:, :, self.numeric_indices]  # (batch, seq_len, num_numeric_features)

        # embedding and concat
        embedded = [emb(cat) for emb, cat in zip(self.embeddings, x_cat)]  # each: (batch, seq_len, emb_dim)
        x_emb = torch.cat(embedded, dim=-1) if embedded else torch.zeros_like(x_num[:, :, :0])
        x_combined = torch.cat([x_num, x_emb], dim=-1)  # (batch, seq_len, effective_input_dim)

        lstm_out, _ = self.lstm(x_combined)  # (batch, seq_len, lstm_dim)
        attn_out, _ = self.attn(lstm_out, lstm_out, lstm_out)  # (batch, seq_len, lstm_dim)
        x = self.norm1(lstm_out + attn_out)

        ffn_out = self.ffn(x)
        x = self.norm2(x + ffn_out)

        x = x.permute(0, 2, 1)  # (batch, lstm_dim, seq_len)
        x = self.global_pool(x).squeeze(-1)  # (batch, lstm_dim)
        x = torch.sigmoid(self.fc1(x))
        x = self.dropout(x)
        x = self.fc2(x)
        out = self.activation(x)
        return out  # (batch, 1)


class TimeSeriesDataset(Dataset):
    def __init__(self, x_seq: torch.Tensor, y_seq: torch.Tensor):
        self.x = x_seq.float()
        self.y = y_seq.float().unsqueeze(1) if y_seq.ndim == 1 else y_seq.float()

    def __len__(self):
        return len(self.y)

    def __getitem__(self, idx):
        return self.x[idx], self.y[idx]


class LSTMTransformerWrapper:
    def __init__(self,
                 total_input_dim: int,
                 cat_index: Dict[int, int],
                 seq_len: int,
                 lr: float = 1e-3,
                 device: str = 'cuda' if torch.cuda.is_available() else 'cpu',
                 early_stopping_patience: int = 10,
                 criterion = nn.MSELoss(),
                 **model_kwargs):

        self.device = device
        self.seq_len = seq_len
        self.cat_index = cat_index
        self.total_input_dim = total_input_dim
        self.early_stopping_patience = early_stopping_patience

        self.model = LSTMTransformer(
            total_input_dim=total_input_dim,
            cat_index=cat_index,
            seq_len=seq_len,
            **model_kwargs
        ).to(self.device)
        self.criterion = criterion
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=lr)

    def _create_sequences_from_dataframe(self, X_df: pd.DataFrame, y_df: pd.Series):
        X, Y = [], []
        X_array = X_df.values.astype(np.float32)
        y_array = y_df.values.astype(np.float32)
        for i in range(len(X_df) - self.seq_len + 1):
            X.append(X_array[i:i+self.seq_len])
            Y.append(y_array[i+self.seq_len-1])
        return torch.tensor(np.array(X)), torch.tensor(np.array(Y))

    def fit(self,
            x_train: torch.Tensor,
            y_train: torch.Tensor,
            x_val: Optional[torch.Tensor] = None,
            y_val: Optional[torch.Tensor] = None,
            epochs: int = 100,
            batch_size: int = 64,
            verbose: bool = True):

        train_dataset = TimeSeriesDataset(x_train, y_train)
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

        val_loader = None
        if x_val is not None and y_val is not None:
            val_dataset = TimeSeriesDataset(x_val, y_val)
            val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)

        best_loss = float('inf')
        patience = 0
        best_state = None

        for epoch in range(epochs):
            self.model.train()
            total_loss = 0.0
            for x_batch, y_batch in train_loader:
                x_batch = x_batch.to(self.device)
                y_batch = y_batch.to(self.device)

                self.optimizer.zero_grad()
                y_pred = self.model(x_batch)
                loss = self.criterion(y_pred, y_batch)
                loss.backward()
                self.optimizer.step()
                total_loss += loss.item() * x_batch.size(0)

            avg_loss = total_loss / len(train_loader.dataset)

            if verbose:
                print(f"Epoch [{epoch+1}/{epochs}] Train Loss: {avg_loss:.4f}", end='')

            if val_loader is not None:
                self.model.eval()
                val_loss = 0.0
                with torch.no_grad():
                    for x_batch, y_batch in val_loader:
                        x_batch = x_batch.to(self.device)
                        y_batch = y_batch.to(self.device)
                        y_pred = self.model(x_batch)
                        loss = self.criterion(y_pred, y_batch)
                        val_loss += loss.item() * x_batch.size(0)
                val_loss /= len(val_loader.dataset)
                if verbose:
                    print(f" | Val Loss: {val_loss:.4f}")

                if val_loss < best_loss:
                    best_loss = val_loss
                    best_state = self.model.state_dict()
                    patience = 0
                else:
                    patience += 1
                    if patience >= self.early_stopping_patience:
                        if verbose:
                            print("Early stopping triggered.")
                        break
            else:
                if verbose:
                    print("")

        if best_state:
            self.model.load_state_dict(best_state)
            self.model.train()

    def fit_dataframe(self, X_df: pd.DataFrame, y_df: pd.Series,
                      val_split: float = 0.2, **kwargs):
        X_seq, Y_seq = self._create_sequences_from_dataframe(X_df, y_df)
        val_size = int(len(X_seq) * val_split)
        x_train, y_train = X_seq[:-val_size], Y_seq[:-val_size]
        x_val, y_val = X_seq[-val_size:], Y_seq[-val_size:]
        self.fit(x_train, y_train, x_val, y_val, **kwargs)

    def predict(self, x_input: torch.Tensor, batch_size: int = 64) -> torch.Tensor:
        self.model.eval()
        preds = []
        loader = DataLoader(x_input, batch_size=batch_size)

        with torch.no_grad():
            for x_batch in loader:
                x_batch = x_batch.to(self.device)
                y_pred = self.model(x_batch)
                preds.append(y_pred.cpu())

        return torch.cat(preds, dim=0)

    def predict_dataframe(self, X_df: pd.DataFrame) -> torch.Tensor:
        X_seq, _ = self._create_sequences_from_dataframe(X_df, pd.Series(np.zeros(len(X_df))))
        return self.predict(X_seq)

    def save(self, path: str):
        torch.save(self.model.state_dict(), path)

    def load(self, path: str):
        self.model.load_state_dict(torch.load(path, map_location=self.device))
        self.model.to(self.device)
