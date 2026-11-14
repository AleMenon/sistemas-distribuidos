import { useState, type FormEvent } from "react";
import { signPromotion, cancelAllPromotions } from "../services/promotions";

const categories = ["Informática", "Periféricos", "Acessórios"];

function PromotionsForm() {
  const [email, setEmail] = useState("");
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  function toggleCategory(category: string) {
    setSelectedCategories((current) =>
      current.includes(category)
        ? current.filter((item) => item !== category)
        : [...current, category],
    );
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (selectedCategories.length === 0) {
      setMessage("Selecione pelo menos uma categoria.");
      return;
    }

    setLoading(true);
    setMessage("");

    try {
      const responses = await Promise.all(
        selectedCategories.map((category) =>
          signPromotion({
            email,
            category,
          }),
        ),
      );

      if (responses.every((response) => response.status === "APROVADO")) {
        setMessage("Inscrição recebida com sucesso!");
        setEmail("");
        setSelectedCategories([]);
      } else {
        setMessage("Solicitação enviada.");
      }
    } catch (error) {
      console.error(error);
      setMessage("Erro ao realizar inscrição.");
    } finally {
      setLoading(false);
    }
  }

  async function handleCancel() {
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim())) {
      setMessage("Informe um e-mail válido.");
      return;
    }

    setLoading(true);
    setMessage("");

    try {
      await cancelAllPromotions(email.trim());

      setMessage("Solicitação de cancelamento enviada!");
      setSelectedCategories([]);
    } catch (error) {
      console.error(error);
      setMessage("Erro ao cancelar inscrições.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <section>
      <h2>Receba nossas promoções</h2>

      <form onSubmit={handleSubmit}>
        <div>
          <label htmlFor="email">E-mail:</label>

          <input
            id="email"
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            placeholder="seuemail@exemplo.com"
            required
          />
        </div>

        <div>
          <h3>Categorias de interesse</h3>

          {categories.map((category) => (
            <label key={category}>
              <input
                type="checkbox"
                checked={selectedCategories.includes(category)}
                onChange={() => toggleCategory(category)}
              />
              {category}
            </label>
          ))}
        </div>

        <button type="submit" disabled={loading}>
          {loading ? "Enviando..." : "Cadastrar"}
        </button>

        <button type="button" onClick={handleCancel} disabled={loading}>
          Cancelar todas as inscrições
        </button>
      </form>

      {message && <p>{message}</p>}
    </section>
  );
}

export default PromotionsForm;
