import z from "zod";
import { Company, ModelId, Organization } from "./process_firestore";
import { PriceRecord, SubscriptionItemRecord } from "./stripe_data";

export async function migrate() {
  // stripe
  const subscriptionItemsFile = await Bun.file(
    "data/stripe_subscription_items.json"
  ).json();
  const subscriptionItems = z.array(SubscriptionItemRecord).parse(subscriptionItemsFile);
  const companySubscriptionItems = Map.groupBy(subscriptionItems, (si) => si.customerId);

  const pricesFile = await Bun.file("data/stripe_prices.json").json();
  const prices = z.array(PriceRecord).parse(pricesFile);
  const priceMap = Map.groupBy(prices, (p) => p.product_id);

  // organizations
  const organizationsFile = await Bun.file(
    "data/processed_organizations.json"
  ).json();
  const organizations = z.array(Organization).parse(organizationsFile);
  const companyOrgs = Map.groupBy(organizations, (o) => o.companyId);

  // companies
  const companiesFile = await Bun.file("data/processed_companies.json").json();
  const companies = z.array(Company).parse(companiesFile);

  const stripeCompanies = companies
    .filter(
      (c) =>
        c.stripeCustomerId ||
        c.stripeSubscriptionId ||
        c.stripeSubscriptionStatus
    )
    .filter((c) => companyOrgs.has(c.id));

  const statusGroups = Map.groupBy(
    stripeCompanies,
    (c) => c.stripeSubscriptionStatus
  );

  console.log(stripeCompanies.length);
  for (const [status, group] of statusGroups) {
    console.log(`${status}: ${group.length}`);
  }

  const modelIds = new Set<string>();
  for (const org of organizations) {
    for (const modelId of org.modelIds) {
      modelIds.add(modelId);
    }
  }

  /**
   * 
   * Process companies
   * 
   */

  const res = []
  
  const companiesWithOrgs = companies.filter(c => companyOrgs.has(c.id))

  for (const company of companiesWithOrgs) {
    const orgs = companyOrgs.get(company.id)!;
    const subscriptionItems = company.stripeCustomerId ? companySubscriptionItems.get(company.stripeCustomerId) || [] : [];
    
    const row = calculateMigrationScenarios(company, orgs, subscriptionItems);
    res.push(row);
  }

  analyzeMigrationScenarios(res);

  Bun.write("data/migrate.json", JSON.stringify(res, null, 2));
}

type MigrationResult = ReturnType<typeof calculateMigrationScenarios>;

function analyzeMigrationScenarios(data: MigrationResult[]) {
  console.log("\n--- Migration Analysis ---\n");

  // Helper functions
  const calculateMean = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const calculateMedian = (arr: number[]) => {
    const sorted = [...arr].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  };

  // 1. ARR Difference (Current vs. Cheapest)
  const payingCustomers = data.filter(d => d.currArr > 0);
  const arrDiffs = payingCustomers.map(d => d.leastArrDiff);
  const meanArrDiff = calculateMean(arrDiffs);
  const medianArrDiff = calculateMedian(arrDiffs);

  console.log("Insight: ARR Change (Current vs. Cheapest Plan)");
  console.log(`- Mean ARR Change: $${meanArrDiff.toFixed(2)}`);
  console.log(`- Median ARR Change: $${medianArrDiff.toFixed(2)}`);
  console.log("  (Negative values indicate an average saving for the customer)\n");

  const totalArrBefore = payingCustomers.reduce((sum, d) => sum + d.currArr, 0);
  const totalArrAfter = payingCustomers.reduce((sum, d) => sum + d.leastArr, 0);
  console.log(`- Total ARR Before: $${totalArrBefore.toFixed(2)}`);
  console.log(`- Total ARR with Cheapest Plan: $${totalArrAfter.toFixed(2)}`);
  console.log(`- Net ARR Change: $${(totalArrAfter - totalArrBefore).toFixed(2)}\n`);


  // 2. Leftover Credits (Matched ARR Scenario)
  const matchScenarioCustomers = data.filter(d => d.matchPlan !== null);
  const leftoverCredits = matchScenarioCustomers.map(d => d.matchLeftOverCredits);
  const meanLeftoverCredits = calculateMean(leftoverCredits);
  const medianLeftoverCredits = calculateMedian(leftoverCredits);
  
  console.log("Insight: Leftover Credits (Matched ARR Scenario)");
  console.log(`- Mean Leftover Credits: ${meanLeftoverCredits.toFixed(0)}`);
  console.log(`- Median Leftover Credits: ${medianLeftoverCredits.toFixed(0)}\n`);

  // 3. Plan Distribution
  const leastPlanCounts = data.reduce((acc, d) => {
    acc[d.leastArrPlan] = (acc[d.leastArrPlan] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);
  
  const matchPlanCounts = matchScenarioCustomers.reduce((acc, d) => {
    const plan = d.matchPlan!;
    acc[plan] = (acc[plan] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  console.log("Insight: New Plan Distribution");
  console.log("- Cheapest Plan Choices:", leastPlanCounts);
  console.log("- Matched ARR Plan Choices:", matchPlanCounts, "\n");

  // 4. Segmentation: Brands vs. Agencies
  const brands = data.filter(d => d.type === 'IN_HOUSE');
  const agencies = data.filter(d => d.type === 'AGENCY');
  
  const brandArrDiffs = brands.filter(b => b.currArr > 0).map(b => b.leastArrDiff);
  const agencyArrDiffs = agencies.filter(a => a.currArr > 0).map(a => a.leastArrDiff);

  console.log("Insight: Brand vs. Agency Analysis (Cheapest Plan)");
  if (brandArrDiffs.length > 0) {
    console.log(`- Brands Mean ARR Change: $${calculateMean(brandArrDiffs).toFixed(2)}`);
  }
  if (agencyArrDiffs.length > 0) {
    console.log(`- Agencies Mean ARR Change: $${calculateMean(agencyArrDiffs).toFixed(2)}`);
  }
  console.log("\n--------------------------\n");
}


export function calculateMigrationScenarios(
  company: Pick<Company, 'type' | 'name' | 'domain'>,
  orgs: Pick<Organization, 'modelIds' | 'promptsCount'>[],
  subscriptionItems: Pick<SubscriptionItemRecord, 'mrrCents'>[]
) {
  const currArr = subscriptionItems.reduce((acc, si) => acc + si.mrrCents, 0) * 12 / 100;

  const credits = orgs.map(org => {
    const modelPrices = org.modelIds.map(modelId => ModelIdPriceMap[modelId as ModelId]);
    return modelPrices.reduce((acc, price) => acc + price, 0) * org.promptsCount;
  });
  const creditsAllocated = credits.reduce((acc, credit) => acc + credit, 0);

  const plans = company.type === 'IN_HOUSE' ? brandPlans : agencyPlans;
  const planEntries = Object.entries(plans);

  // 1. Corrected Least cost scenario
  const leastCostOptions = planEntries.map(([planName, plan]) => {
    const annualPlanCredits = plan.credits * 12;
    const extraCreditsNeeded = Math.max(0, creditsAllocated - annualPlanCredits);
    const annualPlanPrice = plan.price * 12;
    const cost = annualPlanPrice + extraCreditsNeeded * plan.pricePerCredit;
    const totalCredits = annualPlanCredits + extraCreditsNeeded;
    return {
      planName,
      cost,
      extraCredits: extraCreditsNeeded,
      leftover: totalCredits - creditsAllocated
    };
  });

  const bestLeastCostOption = leastCostOptions.reduce((best, current) =>
    (current.cost < best.cost) ? current : best
  );

  const leastArr = bestLeastCostOption.cost / 100;
  const leastArrPlan = bestLeastCostOption.planName;
  const leastArrCreditsQuantity = bestLeastCostOption.extraCredits;
  const leastArrLeftover = bestLeastCostOption.leftover;

  // 2. Matched ARR scenario
  const suitablePlans = planEntries.filter(([, plan]) => plan.price * 12 < currArr * 100);
  const bestMatchPlanEntry = suitablePlans.reduce((best, current) => {
    return (best && best[1].price > current[1].price) ? best : current;
  }, null as [string, (typeof brandPlans[keyof typeof brandPlans]) | (typeof agencyPlans[keyof typeof agencyPlans])] | null);

  let matchPlanName: keyof typeof plans | null = null;
  let matchArr = 0;
  let matchLeftOverCredits = 0;
  let matchQuantity = 0;

  if (bestMatchPlanEntry) {
    matchPlanName = bestMatchPlanEntry[0] as keyof typeof plans;
    const bestMatchPlan = bestMatchPlanEntry[1];
    const remainingArr = currArr * 100 - (bestMatchPlan.price * 12);
    matchQuantity = Math.ceil(remainingArr / bestMatchPlan.pricePerCredit);
    matchArr = ((bestMatchPlan.price * 12) + matchQuantity * bestMatchPlan.pricePerCredit) / 100;
    matchLeftOverCredits = ((bestMatchPlan.credits * 12) + matchQuantity) - creditsAllocated;
  }

  return {
    name: company.name,
    domain: company.domain,
    type: company.type,
    currArr,
    creditsAllocated,

    leastArr,
    leastArrPlan,
    leastArrCreditsQuantity,
    leastArrLeftover,
    leastArrDiff: leastArr - currArr,

    matchArr,
    matchPlan: matchPlanName,
    matchQuantity,
    matchLeftOverCredits,
  };
}

// companyName, currArr, creditsAllocated, leastArr, leastArrPlan, leastArrCreditsQuantity, leastArrLeftover, matchArr, matchPlan, matchQuantity, matchLeftOverCredits

export const brandPlans = {
  starter: {
    price: 8900,
    credits: 4450,
    
    pricePerCredit: 8900 / 4450,
  },
  pro: {
    price: 24900,
    credits: 18675,
    pricePerCredit: 24900 / 18675,
  },
  enterprise: {
    price: 49900,
    credits: 49900,
    pricePerCredit: 49900 / 49900,
  },
} as const;

export const agencyPlans = {
  intro: {
    price: 29900,
    credits: 14950,
    pricePerCredit: 29900 / 14950,
  },
  growth: {
    price: 49900,
    credits: 37425,
    pricePerCredit: 49900 / 37425,
  },
  scale: {
    price: 60000,
    credits: 60000,
    pricePerCredit: 60000 / 60000,
  },
} as const;

export const ModelIdPriceMap: Record<ModelId, number> = {
  "gpt-4o": 1,
  "chatgpt": 1,
  "sonar": 1,
  "google-ai-overview": 1,
  "llama-3-3-70b-instruct": 0.5,
  "gpt-4o-search": 1,
  "claude-sonnet-4": 3,
  "claude-3-5-haiku": 3,
  "gemini-1-5-flash": 1,
  "deepseek-r1": 1,
  "gemini-2-5-flash": 3,
  "google-ai-mode": 1,
  "grok-2-1212": 3,
  "gpt-3-5-turbo": 1,
} as const;