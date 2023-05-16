import { extractMembers } from "@treecg/ldes-snapshot";
import { storeFromFile, TSMongoDBIngestor } from "@treecg/ldes-timeseries";
import { TimeBucketizerFactory } from "@treecg/timeBased-bucketizer";
import  { TimeBucketizer } from "@treecg/timeBased-bucketizer";
async function main() {
    // load some members
    const fileName = "/home/hannes/WebstormProjects/Masterproef/LDES-timeseries/data/location-LDES.ttl"
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


    // Create a new Bucketizer instance using the TimeBucketizerFactory
    const options = {
        pageSize: 50,
        type: 'http://www.w3.org/2001/XMLSchema#dateTime'
    };

    console.log(options);

    const bucketizer = new TimeBucketizer(options, [new TimeBucketizerFactory()]);
    // Bucketize the members using the bucketizer
    const bucketizedMembers = await bucketizer.bucketize(members);
    const ingestor = new TSMongoDBIngestor({ streamIdentifier: streamIdentifier, viewDescriptionIdentifier: viewDescriptionIdentifier });

    await ingestor.instantiate(ldesTSConfig);
    await ingestor.publish(bucketizedMembers);

    await ingestor.exit();
}
main();