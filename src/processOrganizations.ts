import { JSONParser } from "@streamparser/json";

interface Organization {
  id: string;
  promptLimit?: number;
  chatIntervalInHours?: number;
  domain?: string;
  name?: string;
  status?: string;
  modelIds?: string[];
  companyName?: string;
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
