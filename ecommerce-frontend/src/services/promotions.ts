interface Promotion {
  email: string;
  category: string;
}

export async function signPromotion(promotion: Promotion) {
  const response = await fetch("/v1/promotion/sign", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(promotion),
  });

  if (!response.ok) {
    throw new Error("Erro ao cadastrar promoção");
  }

  return response.json();
}
