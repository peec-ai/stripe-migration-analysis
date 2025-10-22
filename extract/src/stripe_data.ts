import Stripe from "stripe";
import z from "zod";

// 2. Define a clear type for the data we want to save
export const SubscriptionItemRecord = z.object({
  customerId: z.string(),
  subscriptionId: z.string(),
  subscriptionItemId: z.string(),
  planId: z.string(),
  interval: z.string(),
  intervalCount: z.number(),
  mrrCents: z.number(),
  unitAmount: z.number(),
  quantity: z.number(),
  promptLimit: z.number().optional(),
  discounts: z.array(z.string()), // Item-level coupon IDs
  subscriptionDiscounts: z.array(z.string()), // Subscription-level coupon IDs
});
export type SubscriptionItemRecord = z.infer<typeof SubscriptionItemRecord>;

export async function fetchStripeSubscriptions() {
  console.log("Connecting to Stripe to fetch all active subscriptions...");

  const allSubs = [];

  const stripe = new Stripe(process.env.STRIPE_API_KEY!, {
    apiVersion: "2025-08-27.basil",
  });

  try {
    // 3. Use the auto-paging iterator. This is the key to handling pagination.
    // Stripe's library handles all the 'starting_after' logic for you.
    for await (const sub of stripe.subscriptions.list({
      status: "active",
      limit: 100, // Fetch 100 per API call
      expand: ["data.items.data.price", "data.discounts.coupon", "data.items.data.discounts"],
    })) {
      allSubs.push(sub);
    }

    console.log(
      `Successfully fetched ${allSubs.length} active subscription items.`
    );

    // 4. Write the entire array to a JSON file
    const outputDir = "../data";
    const outputFilename = "stripe_subscriptions.json";
    // JSON.stringify with a space argument of 2 makes the file readable (pretty-print)
    await Bun.write(
      `${outputDir}/${outputFilename}`,
      JSON.stringify(allSubs, null, 2)
    );

    console.log(`✅ Data saved to ${outputFilename}`);
  } catch (error) {
    if (error instanceof Stripe.errors.StripeError) {
      console.error("Stripe API Error:", error.message);
    } else {
      console.error("An unexpected error occurred:", error);
    }
  }
}

export const ProductRecord = z.object({
  id: z.string(),
  name: z.string(),
  description: z.string().nullable(),
});
export type ProductRecord = z.infer<typeof ProductRecord>;

export async function fetchProducts() {
  console.log("Connecting to Stripe to fetch all active products...");

  const stripe = new Stripe(process.env.STRIPE_API_KEY!, {
    apiVersion: "2025-08-27.basil",
  });

  const allProducts = [];
  try {
    for await (const product of stripe.products.list({
      active: true,
      limit: 100,
    })) {
      allProducts.push(product);
    }
    for await (const product of stripe.products.list({
      active: false,
      limit: 100,
    })) {
      allProducts.push(product);
    }
    console.log(`Successfully fetched ${allProducts.length} products.`);
    const outputDir = "../data";
    const outputFilename = "stripe_products.json";
    await Bun.write(
      `${outputDir}/${outputFilename}`,
      JSON.stringify(allProducts, null, 2)
    );
    console.log(`✅ Data saved to ${outputFilename}`);
  } catch (error) {
    if (error instanceof Stripe.errors.StripeError) {
      console.error("Stripe API Error:", error.message);
    } else {
      console.error("An unexpected error occurred:", error);
    }
  }
}
