export interface OrderItem {
  id: number;
  quantity: number;
}

export interface Order {
  id: number;
  items: OrderItem[];
}

export interface OrderResponse {
  status: string;
  payment_url?: string;
}

export async function createOrder(order: Order): Promise<OrderResponse> {
  const response = await fetch("/v1/orders", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(order),
  });

  if (!response.ok) {
    throw new Error("Erro ao criar pedido");
  }

  return response.json();
}
