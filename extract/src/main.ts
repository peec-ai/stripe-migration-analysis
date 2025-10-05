import { fetchPrices, fetchProducts, fetchStripeSubscriptionItems } from "./stripe_data";
import { fetchCompanies, fetchOrganizations } from "./firestore_data";
import { processCompanies, processOrganizations } from "./process_firestore";

async function main() {
  // await fetchStripeSubscriptionItems();
  // await fetchProducts();
  // await fetchPrices();
  await fetchCompanies();
  await fetchOrganizations();
  await processOrganizations();
  await processCompanies();
}

main();