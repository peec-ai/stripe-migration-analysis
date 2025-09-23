import { describe, it, expect } from "vitest";
import { calculateMigrationScenarios } from "./migrate";
import type { ModelId } from "./process_firestore";
import type { SubscriptionItemRecord } from "./stripe_data";

describe("calculateMigrationScenarios", () => {
  it("should calculate the least cost scenario for a brand (IN_HOUSE)", () => {
    const company = { name: "Test Brand", domain: "test.com", type: "IN_HOUSE" as const };
    const orgs = [{ modelIds: ["gpt-4o"] as ModelId[], promptsCount: 5000 }];
    const subscriptionItems = [{ mrrCents: 10000 }] as SubscriptionItemRecord[]; // $100/mo -> $1200 ARR

    const result = calculateMigrationScenarios(company, orgs, subscriptionItems);

    // starter plan is $89/mo for 4450 credits/mo. Annual: $1068 for 53400 credits.
    // Credits needed (5000) is less than annual credits. No extra credits needed.
    expect(result.leastArr).toBe(1068);
    expect(result.leastArrPlan).toBe("starter");
    expect(result.leastArrCreditsQuantity).toBe(0);
    expect(result.leastArrLeftover).toBe(48400); // 53400 - 5000
  });

  it("should calculate the least cost scenario by choosing a larger plan", () => {
    const company = { name: "Test Brand", domain: "test.com", type: "IN_HOUSE" as const };
    // Needs 150,000 credits
    const orgs = [{ modelIds: ["gpt-4o", "chatgpt"] as ModelId[], promptsCount: 75000 }];
    const subscriptionItems = [] as SubscriptionItemRecord[];

    const result = calculateMigrationScenarios(company, orgs, subscriptionItems);

    // Cost of starter + extra: (8900*12) + (150000 - 53400) * 2 = 106800 + 193200 = 300000 cents ($3000)
    // Cost of pro: 24900 * 12 = 298800 cents ($2988)
    // Pro is cheaper
    expect(result.leastArr).toBe(2988);
    expect(result.leastArrPlan).toBe("pro");
    expect(result.leastArrCreditsQuantity).toBe(0);
    expect(result.leastArrLeftover).toBe(224100 - 150000); // 74100
  });

  it("should calculate the matched ARR scenario for an agency", () => {
    const company = { name: "Test Agency", domain: "agency.com", type: "AGENCY" as const };
    const orgs = [{ modelIds: ["gpt-4o"] as ModelId[], promptsCount: 10000 }]; // 10k credits needed
    const subscriptionItems = [{ mrrCents: 30000 }] as SubscriptionItemRecord[]; // $300/mo -> $3600 ARR

    const result = calculateMigrationScenarios(company, orgs, subscriptionItems);

    // Current ARR is $3600.
    // Best plan under is 'intro' at $299/mo -> $3588/yr.
    // Remaining ARR to match: 360000 - 358800 = 1200 cents
    // Price per credit on intro is 2.
    // Extra credits to buy: ceil(1200 / 2) = 600
    // New ARR: (358800 + 600 * 2) / 100 = 3600
    // Leftover credits: (14950 * 12 + 600) - 10000 = 179400 + 600 - 10000 = 170000
    expect(result.matchArr).toBe(3600);
    expect(result.matchPlan).toBe("intro");
    expect(result.matchQuantity).toBe(600);
    expect(result.matchLeftOverCredits).toBe(170000);
  });
});
