import { describe, it, expect } from "vitest";
import { calculateMigrationScenarios } from "./migrate";
import type { ModelId } from "./process_firestore";
import type { SubscriptionItemRecord } from "./stripe_data";

describe("calculateMigrationScenarios", () => {
  it("should calculate the least cost scenario for a brand (IN_HOUSE)", () => {
    const company = { name: "Test Brand", domain: "test.com", type: "IN_HOUSE" as const };
    const orgs = [{ modelIds: ["gpt-4o"] as ModelId[], promptsCount: 5000 }];
    const subscriptionItems = [{ mrrCents: 10000 }]; // $100/mo -> $1200 ARR

    const result = calculateMigrationScenarios(company, orgs, subscriptionItems);

    // starter plan (4450 credits) + 550 extra credits
    // cost = 8900 + 550 * 2 = 8900 + 1100 = 10000
    expect(result.leastArr).toBe(100);
    expect(result.leastArrPlan).toBe("starter");
    expect(result.leastArrCreditsQuantity).toBe(550);
    expect(result.leastArrLeftover).toBe(0);
  });

  it("should calculate the least cost scenario by choosing a larger plan", () => {
    const company = { name: "Test Brand", domain: "test.com", type: "IN_HOUSE" as const };
    // Needs 18000 credits
    const orgs = [{ modelIds: ["gpt-4o", "chatgpt"] as ModelId[], promptsCount: 9000 }];
    const subscriptionItems = [] as SubscriptionItemRecord[];

    const result = calculateMigrationScenarios(company, orgs, subscriptionItems);

    // Cheaper to buy 'pro' (18675 credits @ $249) than 'starter' (4450 credits) + 13550 extra credits.
    // Cost of starter + extra: 8900 + 13550 * 2 = 36000
    // Cost of pro: 24900
    expect(result.leastArr).toBe(249);
    expect(result.leastArrPlan).toBe("pro");
    expect(result.leastArrCreditsQuantity).toBe(0);
    expect(result.leastArrLeftover).toBe(675);
  });

  it("should calculate the matched ARR scenario for an agency", () => {
    const company = { name: "Test Agency", domain: "agency.com", type: "AGENCY" as const };
    const orgs = [{ modelIds: ["gpt-4o"] as ModelId[], promptsCount: 10000 }]; // 10k credits needed
    const subscriptionItems = [{ mrrCents: 30000 }]; // $300/mo -> $3600 ARR

    const result = calculateMigrationScenarios(company, orgs, subscriptionItems);

    // Current ARR is $3600.
    // Smallest plan under is 'intro' at $299.
    // Remaining ARR to match: 3600 - 299 = $3301
    // Price per credit on intro is 2.
    // Extra credits to buy: ceil(3301 / 2) = 1651
    // New ARR: 29900 + 1651 * 2 = 29900 + 3302 = 33202 cents = $332.02 -> THIS IS WRONG.
    // Ah, price per credit is price / credits. Let's re-calculate.
    // intro pricePerCredit = 29900 / 14950 = 2
    // matchQuantity = ceil((360000 - 29900) / 2) = ceil(330100 / 2) = 165050
    // matchArr = (29900 + 165050 * 2) / 100 = (29900 + 330100) / 100 = 3600
    // This seems off. Let's re-read the code.
    // `currArr` is in dollars. `plan.price` is in cents.
    // remainingArr = 3600 * 100 - 29900 = 330100
    // matchQuantity = Math.ceil(330100 / 2) = 165050. This is a huge number of credits.
    // Let me check my understanding of the `match` scenario.
    // "match the exact arr they're currently paying (or the smallest number over that)"
    // Okay, the `matchQuantity` is credits, not cents.
    
    // Let's re-calculate based on the logic in the function.
    // currArr = 3600
    // suitable plan is 'intro' at 29900 cents.
    // remainingArr = 3600 * 100 - 29900 = 330100 cents
    // pricePerCredit for intro is 2.
    // matchQuantity = ceil(330100 / 2) = 165050 credits.
    // matchArr = (29900 + 165050 * 2) / 100 = (29900 + 330100) / 100 = 3600.
    // Okay the ARR matches.
    // Leftover credits = (14950 + 165050) - 10000 = 180000 - 10000 = 170000

    expect(result.matchArr).toBe(3600);
    expect(result.matchPlan).toBe("intro");
    expect(result.matchQuantity).toBe(165050);
    expect(result.matchLeftOverCredits).toBe(170000);
  });
});
