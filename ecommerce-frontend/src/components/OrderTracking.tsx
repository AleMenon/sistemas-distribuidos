export interface CreatedOrder {
  id: number;
  status: string;
  paymentUrl?: string;
}

interface OrderTrackingProps {
  order: CreatedOrder;
}

function OrderTracking({ order }: OrderTrackingProps) {
  return (
    <section>
      <h2>Acompanhamento do pedido</h2>

      <p>Pedido: {order.id}</p>
      <p>Status: {order.status}</p>

      {order.paymentUrl && (
        <a href={order.paymentUrl} target="_blank" rel="noopener noreferrer">
          Realizar pagamento
        </a>
      )}
    </section>
  );
}

export default OrderTracking;
