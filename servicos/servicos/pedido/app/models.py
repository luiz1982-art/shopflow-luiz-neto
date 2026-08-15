import uuid
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

# --- Sub-modelos do Pedido ---
class ItemPedido(BaseModel):
    produto_id: str
    quantidade: int
    preco_unitario: float

class PedidoCreate(BaseModel):
    cliente_id: str
    itens: List[ItemPedido]
    valor_total: float
    forma_pagamento: str  # cartao_credito | pix | boleto

# --- Modelo de Pedido Armazenado ---
class PedidoDB(BaseModel):
    pedido_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    cliente_id: str
    itens: List[ItemPedido]
    valor_total: float
    forma_pagamento: str
    status: str = "criado"  # criado | confirmado | cancelado
    pagamento_ok: Optional[bool] = None
    fraude_ok: Optional[bool] = None
    criado_em: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

# --- Envelope Padrão de Evento ---
class Envelope(BaseModel):
    evento_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    evento_tipo: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: str
    versao_schema: str = "1.0"
    payload: dict