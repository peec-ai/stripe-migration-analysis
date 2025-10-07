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
  discounts: z.array(z.string()),
  subscriptionDiscounts: z.array(z.string()),
});
export type SubscriptionItemRecord = z.infer<typeof SubscriptionItemRecord>;

export async function fetchStripeSubscriptionItems() {
  console.log("Connecting to Stripe to fetch all active subscription items...");

  const allItems: SubscriptionItemRecord[] = [];

  const stripe = new Stripe(process.env.STRIPE_API_KEY!, {
    apiVersion: "2025-08-27.basil",
  });

  try {
    // 3. Use the auto-paging iterator. This is the key to handling pagination.
    // Stripe's library handles all the 'starting_after' logic for you.
    for await (const sub of stripe.subscriptions.list({
      status: "active",
      limit: 100, // Fetch 100 per API call
      expand: ["data.items.data.price"], // CRUCIAL: This fetches the full Price object
    })) {
      for (const item of sub.items.data) {
        // Safety check for items without a price or unit amount
        if (!item.price || typeof item.price.unit_amount !== "number") {
          console.warn(`Skipping item ${item.id} due to missing price info.`);
          continue;
        }

        if (!item.price.recurring) {
          console.warn(
            `Skipping item ${item.id} due to missing recurring info.`
          );
          continue;
        }

        const interval = item.price.recurring.interval;
        const intervalCount = item.price.recurring.interval_count;

        const quotient = interval === "year" ? 12 : 1;

        const mrrCents = item.price.unit_amount / (intervalCount * quotient);

        allItems.push({
          customerId: sub.customer as string,
          subscriptionId: sub.id,
          subscriptionItemId: item.id,
          planId: item.price.id,
          interval,
          intervalCount,
          mrrCents,
          unitAmount: item.price.unit_amount,
          quantity: item.quantity || 1,
          subscriptionDiscounts: (sub.discounts as string[]) || [],
          discounts: (item.discounts as string[]) || [],
        });
      }
    }

    console.log(
      `Successfully fetched ${allItems.length} active subscription items.`
    );

    // 4. Write the entire array to a JSON file
    const outputDir = "../data";
    const outputFilename = "stripe_subscription_items.json";
    // JSON.stringify with a space argument of 2 makes the file readable (pretty-print)
    await Bun.write(
      `${outputDir}/${outputFilename}`,
      JSON.stringify(allItems, null, 2)
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

export const PriceRecord = z.object({
  id: z.string(),
  product_id: z.string(),
  currency: z.string(),
  unit_amount: z.number(),
  type: z.string(),
});
export type PriceRecord = z.infer<typeof PriceRecord>;

export async function fetchPrices() {
  console.log("Connecting to Stripe to fetch all active prices...");

  const stripe = new Stripe(process.env.STRIPE_API_KEY!, {
    apiVersion: "2025-08-27.basil",
  });

  const allPrices = [];
  try {
    for await (const price of stripe.prices.list({
      active: true,
      limit: 100,
    })) {
      allPrices.push(price);
    }
    console.log(`Successfully fetched ${allPrices.length} prices.`);
    const outputDir = "../data";
    const outputFilename = "stripe_prices.json";
    await Bun.write(
      `${outputDir}/${outputFilename}`,
      JSON.stringify(allPrices, null, 2)
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

export async function fetchStripeCoupons() {
  console.log("Connecting to Stripe to fetch all coupons...");

  const stripe = new Stripe(process.env.STRIPE_API_KEY!, {
    apiVersion: "2025-08-27.basil",
  });

  const allCoupons = [];
  try {
    for await (const coupon of stripe.coupons.list({
      limit: 100,
    })) {
      allCoupons.push(coupon);
    }
    console.log(`Successfully fetched ${allCoupons.length} coupons.`);
    const outputDir = "../data";
    const outputFilename = "stripe_coupons.json";
    await Bun.write(
      `${outputDir}/${outputFilename}`,
      JSON.stringify(allCoupons, null, 2)
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
