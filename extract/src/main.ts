import {
  fetchProducts,
  fetchStripeSubscriptions,
  fetchStripeCoupons,
} from "./stripe_data";
import { fetchCompanies, fetchOrganizations } from "./firestore_data";
import { processCompanies, processOrganizations } from "./process_firestore";

async function main() {
  await fetchStripeSubscriptions();
  await fetchStripeCoupons();
  await fetchProducts();
  await fetchCompanies();
  await fetchOrganizations();
  await processOrganizations();
  await processCompanies();
}

main();