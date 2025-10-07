import { JSONParser } from "@streamparser/json";
import { z } from "zod";
import fs from "fs";
import path from "path";

export const ModelId = z.enum([
  "gpt-4o",
  "chatgpt",
  "sonar",
  "google-ai-overview",
  "llama-3-3-70b-instruct",
  "gpt-4o-search",
  "claude-sonnet-4",
  "claude-3-5-haiku",
  "gemini-1-5-flash",
  "deepseek-r1",
  "gemini-2-5-flash",
  "google-ai-mode",
  "grok-2-1212",
  "gpt-3-5-turbo",
]);
export type ModelId = z.infer<typeof ModelId>;

export const Organization = z.object({
  id: z.string(),
  promptLimit: z.number(),
  chatIntervalInHours: z.number(),
  domain: z.string().optional(),
  name: z.string(),
  status: z.string(),
  modelIds: z.array(ModelId),
  companyName: z.string(),
  promptsCount: z.number(),
  companyId: z.string(),
});
export type Organization = z.infer<typeof Organization>;

export async function processOrganizations() {
  console.log("Starting to process organizations...");

  const inputFile = path.join(
    __dirname,
    "../../data/firestore_organizations.json"
  );
  const outputFile = path.join(
    __dirname,
    "../../data/processed_organizations.json"
  );

  try {
    const fileContent = fs.readFileSync(inputFile, "utf-8");
    const organizations = JSON.parse(fileContent);

    const processedOrgs = organizations.map((org: any) => {
      const processedOrg: Partial<Organization> = {
        id: org.id,
        promptLimit: org.promptLimit,
        chatIntervalInHours: org.chatIntervalInHours,
        domain: org.domain,
        name: org.name,
        status: org.status,
        modelIds: org.modelIds,
        companyName: org.companyName,
        promptsCount: org.promptsCount,
        companyId: org.companyId,
      };
      return processedOrg;
    });

    fs.writeFileSync(outputFile, JSON.stringify(processedOrgs, null, 2));

    console.log(
      `Successfully processed ${processedOrgs.length} organizations.`
    );
  } catch (error) {
    console.error("Error processing organizations:", error);
  }
}

export const Company = z.object({
  id: z.string(),
  name: z.string(),
  domain: z.string().optional(),
  type: z.enum(["IN_HOUSE", "AGENCY", "PARTNER"]),
  leadType: z.enum(["SALES", "SELF_SERVICE"]).optional(),
  stripeCustomerId: z.string().optional(),
  stripeSubscriptionStatus: z
    .enum(["canceled", "past_due", "active", "trialing"])
    .optional(),
  stripeSubscriptionId: z.string().optional(),
});
export type Company = z.infer<typeof Company>;

export async function processCompanies() {
  console.log("Starting to process companies...");

  const inputFile = path.join(__dirname, "../../data/firestore_companies.json");
  const outputFile = path.join(
    __dirname,
    "../../data/processed_companies.json"
  );

  try {
    const fileContent = fs.readFileSync(inputFile, "utf-8");
    const companies = JSON.parse(fileContent);

    const processedCompanies = companies
      .filter((comp: any) => !comp.isDeleted)
      .map((comp: any) => {
        const processedComp: Company = {
          id: comp.id,
          name: comp.name,
          domain: comp.domain,
          type: comp.type,
          leadType: comp.leadType,
          stripeCustomerId: comp.stripeCustomerId,
          stripeSubscriptionStatus: comp.stripeSubscriptionStatus,
          stripeSubscriptionId: comp.stripeSubscriptionId,
        };
        return processedComp;
      });

    fs.writeFileSync(
      outputFile,
      JSON.stringify(processedCompanies, null, 2)
    );

    console.log(
      `Successfully processed ${processedCompanies.length} companies.`
    );
  } catch (error) {
    console.error("Error processing companies:", error);
  }
}
