import { useEffect, useRef, useState } from "react";
import { getProducts } from "./services/products";
import type { Product } from "./types/Product";
import { createOrder } from "./services/orders";
import PromotionsForm from "./components/PromotionsForm";
import ProductList from "./components/ProductList";
import Cart from "./components/Cart";
import OrderTracking, { type CreatedOrder } from "./components/OrderTracking";

function App() {
  const [products, setProducts] = useState<Product[]>([]);
  useEffect(() => {
    getProducts()
      .then((data) => setProducts(data))
      .catch((error) => console.error(error));
  }, []);

  const [cart, setCart] = useState<Record<number, number>>({});

  function addToCart(product: Product) {
    setCart((current) => {
      const quantity = current[product.id] ?? 0;

      if (quantity >= product.stock) {
        return current;
      }

      return {
        ...current,
        [product.id]: quantity + 1,
      };
    });
  }

  function removeFromCart(productId: number) {
    setCart((current) => {
      const quantity = current[productId] ?? 0;

      return {
        ...current,
        [productId]: Math.max(0, quantity - 1),
      };
    });
  }

  const [submitting, setSubmitting] = useState(false);

  const [createdOrder, setCreatedOrder] = useState<CreatedOrder | null>(null);

  const [orderHistory, setOrderHistory] = useState<CreatedOrder[]>([]);

  async function finishOrder() {
    const items = Object.entries(cart)
      .filter(([, quantity]) => quantity > 0)
      .map(([id, quantity]) => ({
        id: Number(id),
        quantity,
      }));

    if (items.length === 0 || submitting || createdOrder) return;

    const orderId = Date.now();

    setSubmitting(true);
    setCreatedOrder({
      id: orderId,
      status: "CONECTANDO",
    });

    const source = new EventSource(`/v1/sse/${orderId}`);
    sseRef.current = source;

    const statuses: Record<string, string> = {
      "pedido.estoque_ok": "Estoque confirmado",
      "estoque.indisponivel": "Estoque indisponível",
      "pagamento.aprovado": "Pagamento aprovado",
      "pagamento.recusado": "Pagamento recusado",
      "pedido.enviado": "Pedido enviado",
    };

    source.onmessage = (message) => {
      try {
        const data = JSON.parse(message.data);

        if (String(data.content?.id) !== String(orderId)) return;

        if (data.event === "pagamento.checkout_criado") {
          const paymentUrl = data.content?.payment_url;

          if (typeof paymentUrl === "string") {
            setCreatedOrder((current) =>
              current?.id === orderId
                ? {
                    ...current,
                    status: "Aguardando pagamento",
                    paymentUrl,
                  }
                : current,
            );
          }

          return;
        }

        const status = statuses[data.event];

        if (status) {
          setCreatedOrder((current) =>
            current?.id === orderId ? { ...current, status } : current,
          );
        }
      } catch (error) {
        console.error("Erro ao processar SSE:", error);
      }
    };

    try {
      // Aguarda a conexão SSE abrir
      await new Promise<void>((resolve, reject) => {
        let connected = false;

        source.onopen = () => {
          connected = true;
          resolve();
        };

        source.onerror = () => {
          if (!connected) {
            reject(new Error("Não foi possível conectar ao SSE"));
          } else {
            console.warn("Conexão SSE interrompida. Reconectando...");
          }
        };
      });

      setCreatedOrder((current) =>
        current?.id === orderId && current.status === "CONECTANDO"
          ? { ...current, status: "PROCESSANDO" }
          : current,
      );

      // Envia o pedido somente depois de abrir o SSE
      const response = await createOrder({
        id: orderId,
        items,
      });

      setCreatedOrder((current) =>
        current?.id === orderId
          ? {
              ...current,
              status:
                current.status === "CONECTANDO"
                  ? response.status
                  : current.status,
              paymentUrl: response.payment_url,
            }
          : current,
      );
    } catch (error) {
      console.error(error);
      source.close();
      sseRef.current = null;
      setCreatedOrder(null);
      alert("Não foi possível finalizar o pedido.");
    } finally {
      setSubmitting(false);
    }
  }

  function startNewOrder() {
    if (createdOrder) {
      setOrderHistory((current) => [...current, createdOrder]);
    }

    sseRef.current?.close();
    sseRef.current = null;

    setCart({});
    setCreatedOrder(null);
  }

  const sseRef = useRef<EventSource | null>(null);

  useEffect(() => {
    return () => {
      sseRef.current?.close();
    };
  }, []);

  return (
    <main>
      <h1>Minha Loja Virtual</h1>

      <ProductList
        products={products}
        cart={cart}
        onAdd={addToCart}
        onRemove={removeFromCart}
      />

      <Cart
        products={products}
        cart={cart}
        submitting={submitting}
        orderCreated={createdOrder !== null}
        onRemove={removeFromCart}
        onFinish={finishOrder}
      />

      <section>
        <h2>Meus pedidos</h2>

        {orderHistory.map((order) => (
          <OrderTracking key={order.id} order={order} />
        ))}

        {createdOrder && <OrderTracking order={createdOrder} />}

        {createdOrder && !submitting && (
          <button onClick={startNewOrder}>Novo pedido</button>
        )}
      </section>

      <PromotionsForm />
    </main>
  );
}

export default App;
