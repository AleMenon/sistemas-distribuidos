import type { Product } from "../types/Product";

interface ProductListProps {
  products: Product[];
  cart: Record<number, number>;
  onAdd: (product: Product) => void;
  onRemove: (productId: number) => void;
}

function ProductList({ products, cart, onAdd, onRemove }: ProductListProps) {
  return (
    <section>
      <h2>Produtos disponíveis</h2>

      {products.map((product) => (
        <article key={product.id}>
          <h3>{product.name}</h3>

          <p>Estoque: {product.stock}</p>
          <p>No carrinho: {cart[product.id] ?? 0}</p>

          <button
            onClick={() => onAdd(product)}
            disabled={(cart[product.id] ?? 0) >= product.stock}
          >
            Adicionar
          </button>

          <button
            onClick={() => onRemove(product.id)}
            disabled={(cart[product.id] ?? 0) === 0}
          >
            Remover
          </button>
        </article>
      ))}
    </section>
  );
}

export default ProductList;
