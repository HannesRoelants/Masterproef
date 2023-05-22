import {extractMembers} from "@treecg/ldes-snapshot";
import {storeFromFile, TSMongoDBIngestor} from "@treecg/ldes-timeseries";
import {TimeBucketizerFactory, TimeBucketizer} from "./Bucketizer_TimeBased.js";
import {ingest} from "@treecg/sds-storage-writer-mongo";
import {SimpleStream} from "@treecg/connector-types";
import {Parser, Writer, DataFactory} from "n3";
import {SDS} from "@treecg/types";

const {quad, namedNode} = DataFactory;

async function main() {
    // load some members
    const fileName = "C:\\Masterproef\\location-LDES.ttl"
    const ldesIdentifier = "http://localhost:3000/lil/#EventStream"
    const store = await storeFromFile(fileName);
    const members = extractMembers(store, ldesIdentifier);

    const streamIdentifier = "http://example.org/myStream#eventStream"

    // Ingestor Arthur Vercruysse
    const dataSteam = new SimpleStream();
    const metadataStream = new SimpleStream();
    const ingestPromise = ingest(dataSteam, metadataStream, "META", "DATA", "INDEX");

    const metadata = new Parser().parse(`
        @prefix foaf:   <http://xmlns.com/foaf/0.1/> .
        @prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix p-plan: <http://purl.org/net/p-plan#> .
        @prefix prov:   <http://www.w3.org/ns/prov#> .
        @prefix sds:    <https://w3id.org/sds#> .
        @prefix dcat:   <https://www.w3.org/ns/dcat#> .
        @prefix ex:     <http://example.org/ns#>.
        @prefix xsd:    <http://www.w3.org/2001/XMLSchema#>.
        @prefix sh:     <http://www.w3.org/ns/shacl#>.
        @prefix void:   <http://rdfs.org/ns/void#> .
        @prefix ldes:   <https://w3id.org/ldes#>.
        @prefix :       <http://me#> .

        :somePlan a p-plan:Plan;
            rdfs:comment "A epic plan to map csv file to a LDES".

        :dataset a dcat:Dataset;
            dcat:title "Epic dataset";
            dcat:publisher [
                 foaf:name "Arthur Vercruysse"
            ];
            dcat:identifier <http://localhost:3000/ldes>.

        <http://example.org/myStream#eventStream> a sds:Stream;
            p-plan:wasGeneratedBy [
                a p-plan:Activity;
                rdfs:comment "Load in RINF data"
            ];
            sds:carries [ a sds:Member ]; 
            sds:dataset :dataset.
    `   );

    await metadataStream.push(metadata);

    await new Promise(res => setTimeout(res, 2000));

    const factory = new TimeBucketizerFactory();
    const options = {
        type: "time", // Specify the bucketizer type
        lastTimestamp: new Date().getTime(),
        period: 7 * 60 * 1000 // Set the desired period value (e.g., 10 minutes)
    };
    const bucketizer = factory.build(options);

    // Bucketize the members using the bucketizer
    for (let member of members) {
        const extra = await bucketizer.bucketize(member.quads, member.id.value);
        const subject = extra.find(x => x.predicate.equals(SDS.terms.payload)).subject; // zoeken naar quad waarbij de predicate gelijk is aan de SDS payload (verwijst naar het subjextID van u member)
        extra.push(quad(subject, SDS.terms.stream, namedNode(streamIdentifier)))
        const st = new Writer().quadsToString(extra);

        console.log(st)

        const allQuads = [...member.quads, ...extra];
        await dataSteam.push(allQuads);
    }

    await ingestPromise;
    /*
    Ingestor van Wout Slabbinck -> implementatie nog niet volledig gelukt
    const bucketizedMembers = await bucketizer.bucketize(members);
    const ingestor = new TSMongoDBIngestor({ streamIdentifier: streamIdentifier, viewDescriptionIdentifier: viewDescriptionIdentifier });

    await ingestor.instantiate(ldesTSConfig);
    console.log(bucketizedMembers);


    await ingestor.publish(bucketizedMembers);

    await ingestor.exit();

     */
}

main();