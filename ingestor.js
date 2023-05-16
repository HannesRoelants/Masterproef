import { extractMembers } from "@treecg/ldes-snapshot";
import { storeFromFile, TSMongoDBIngestor } from "@treecg/ldes-timeseries";
import { Writer } from "n3";

async function main() {
    // load some members
    const fileName = "C:\\Masterproef\\location-LDES.ttl"
    const ldesIdentifier = "http://localhost:3000/lil/#EventStream"
    const store = await storeFromFile(fileName);
    const members = extractMembers(store, ldesIdentifier);

    const streamIdentifier = "http://example.org/myStream#eventStream"
    const viewDescriptionIdentifier = "http://example.org/myStream#viewDescription"

    const ldesTSConfig = {
        timestampPath: "http://www.w3.org/ns/sosa/resultTime",
        pageSize: 50,
        date: new Date("2022-08-07T08:08:21Z")
    }

    console.log(members);
    const ingestor = new TSMongoDBIngestor({ streamIdentifier: streamIdentifier, viewDescriptionIdentifier: viewDescriptionIdentifier });

    await ingestor.instantiate(ldesTSConfig);
    await ingestor.publish(members)

    await ingestor.exit();
}
main()
