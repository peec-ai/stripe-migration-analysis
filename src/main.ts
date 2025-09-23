import { fetchPrices, fetchProducts, fetchStripeSubscriptionItems } from "./stripe_data";
import { fetchCompanies, fetchOrganizations } from "./firestore_data";
import { countOrganizations } from "./firestore_count";
import { processCompanies, processOrganizations } from "./process_firestore";
import { migrate } from "./migrate";

async function main() {
  // await fetchStripeSubscriptionItems();
  // await fetchProducts();
  // await fetchPrices();
  // await fetchCompanies();
  // await fetchOrganizations();
  // await processOrganizations();
  // await processCompanies();
  await migrate();
}

main();