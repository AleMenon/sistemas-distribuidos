import type { Product } from "../types/Product";

interface CartProps {
  products: Product[];
  cart: Record<number, number>;
  submitting: boolean;
  orderCreated: boolean;
  onRemove: (productId: number) => void;
  onFinish: () => void;
}

function Cart({
  products,
  cart,
  submitting,
  orderCreated,
  onRemove,
  onFinish,
}: CartProps) {
  const selectedProducts = products.filter(
    (product) => (cart[product.id] ?? 0) > 0,
  );

  return (
    <section>
      <h2>Meu carrinho</h2>

      {selectedProducts.length === 0 && <p>Seu carrinho está vazio.</p>}

      {selectedProducts.map((product) => (
        <div key={product.id}>
          <h3>{product.name}</h3>
          <p>Quantidade: {cart[product.id]}</p>

          <button onClick={() => onRemove(product.id)}>
            Remover uma unidade
          </button>
        </div>
      ))}

      <button
        onClick={onFinish}
        disabled={submitting || orderCreated || selectedProducts.length === 0}
      >
        {submitting ? "Enviando..." : "Finalizar pedido"}
      </button>
    </section>
  );
}

export default Cart;
