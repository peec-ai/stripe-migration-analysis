import { fetchPrices, fetchProducts, fetchStripeData } from "./stripe_data";
import { fetchCompanies } from "./firestore_data";

async function main() {
  // await fetchStripeData();
  // await fetchProducts();
  // await fetchPrices();
  await fetchCompanies();
}

main();