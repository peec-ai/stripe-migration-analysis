import { JSONParser } from "@streamparser/json";
import { z } from "zod";

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
  console.log("Starting to process organizations stream...");

  const inputFile = "data/firestore_organizations.json";
  const parser = new JSONParser({
    paths: ["$.*"], // This emits each element of the root array
  });

  const outputFile = "data/processed_organizations.json";
  const writer = Bun.file(outputFile).writer();
  writer.write("[\n");

  let count = 0;

  parser.onValue = ({ value }) => {
    const org = value as any;

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

    // Here you can do something with the processed object
    // For now, we'll just log it.
    // console.log(processedOrg);
    count++;
    writer.write(JSON.stringify(processedOrg, null, 2));
    writer.write(",\n");
  };

  const stream = Bun.file(inputFile).stream();

  for await (const chunk of stream) {
    parser.write(chunk);
  }

  writer.write("\n]\n");
  writer.flush();
  writer.end();

  console.log(`Successfully processed ${count} organizations.`);
}

export const Company = z.object({
  id: z.string(),
  name: z.string(),
  domain: z.string().optional(),
  type: z.enum(["IN_HOUSE", "AGENCY"]),
  leadType: z.enum(["SALES", "SELF_SERVICE"]).optional(),
  stripeCustomerId: z.string().optional(),
  stripeSubscriptionStatus: z
    .enum(["canceled", "past_due", "active", "trialing"])
    .optional(),
  stripeSubscriptionId: z.string().optional(),
});
export type Company = z.infer<typeof Company>;

export async function processCompanies() {
  console.log("Starting to process organizations stream...");

  const inputFile = "../data/firestore_companies.json";
  const parser = new JSONParser({
    paths: ["$.*"], // This emits each element of the root array
  });

  const outputFile = "../data/processed_companies.json";
  const writer = Bun.file(outputFile).writer();
  writer.write("[\n");

  let count = 0;

  parser.onValue = ({ value }) => {
    const org = value as any;
    if (org.isDeleted) {
      return;
    }

    const processedOrg: Company = {
      id: org.id,
      name: org.name,
      domain: org.domain,
      type: org.type,
      leadType: org.leadType,
      stripeCustomerId: org.stripeCustomerId,
      stripeSubscriptionStatus: org.stripeSubscriptionStatus,
      stripeSubscriptionId: org.stripeSubscriptionId,
    };

    // Here you can do something with the processed object
    // For now, we'll just log it.
    // console.log(processedOrg);
    count++;
    writer.write(JSON.stringify(processedOrg, null, 2));
    writer.write(",\n");
  };

  const stream = Bun.file(inputFile).stream();

  for await (const chunk of stream) {
    parser.write(chunk);
  }

  // remove last comma
  writer.write("\n]\n");
  writer.flush();
  writer.end();

  console.log(`Successfully processed ${count} organizations.`);
}
