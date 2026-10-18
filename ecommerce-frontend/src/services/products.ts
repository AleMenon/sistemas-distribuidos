import type { Product } from "../types/Product";

export async function getProducts(): Promise<Product[]> {
  const response = await fetch("/v1/products");

  if (!response.ok) {
    throw new Error("Erro ao buscar produtos");
  }

  return response.json();
}