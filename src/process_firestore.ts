import { JSONParser } from "@streamparser/json";

export interface Organization {
  id: string;
  promptLimit: number;
  chatIntervalInHours: number;
  domain?: string;
  name: string;
  status: string;
  modelIds: string[];
  companyName: string;
  promptsCount: number;
}

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

export interface Company {
  id: string;
  name: string;
  domain?: string;
  type: string;
  leadType?: string;
  stripeCustomerId?: string;
  stripeSubscriptionStatus?: string;
  stripeSubscriptionId?: string;
}

export async function processCompanies() {
  console.log("Starting to process organizations stream...");

  const inputFile = "data/firestore_companies.json";
  const parser = new JSONParser({
    paths: ["$.*"], // This emits each element of the root array
  });

  const outputFile = "data/processed_companies.json";
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
