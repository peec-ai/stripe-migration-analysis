import { fetchPrices, fetchProducts, fetchStripeData } from "./stripe_data";

async function main() {
  // await fetchStripeData();
  await fetchProducts();
  await fetchPrices();
}

main();