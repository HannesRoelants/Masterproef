import type * as RDF from '@rdfjs/types';
import {Factory, parseBucketizerCoreOptions, Partial} from '@treecg/bucketizer-core';
import {BucketizerCore} from '@treecg/bucketizer-core';
import {BucketizerCoreOptions, RelationParameters} from '@treecg/types';
import {RelationType, LDES} from '@treecg/types';

export interface timeBasedBucket{
    startTime: Date;
    endTime: Date;
    pageNumber: number;
}
export class TimeBucketizerFactory implements Factory<BucketizerCoreOptions> {
    type: string = "time";

    build(config: BucketizerCoreOptions, state?: any): BucketizerCore<BucketizerCoreOptions> {
        const total: TimeInputType = Object.assign(config, {lastTimestamp: new Date().getTime()});
        return TimeBucketizer.build(total, state);
    }
    ldConfig(quads: RDF.Quad[], subject: RDF.Term): BucketizerCoreOptions | void {
        const out = parseBucketizerCoreOptions(quads, subject);
        if (out.type.value === LDES.custom(this.type)) {
            return out;
        } else {
            return;
        }
    }
}

export type TimeInputType =
    Partial<BucketizerCoreOptions>
    & { lastTimestamp: number, period?: number, minTimestamp?: Date };

export class TimeBucketizer extends BucketizerCore<TimeInputType> {
    public pageNumber: number;
    public lastTimestamp: Date;
    private readonly period: number;
    public bucketCounter: number;
    public minTimestamp: Date;
    private readonly existingBuckets: timeBasedBucket[] = [];

    private constructor(bucketizerOptions: TimeInputType, state?: any) {
        super(bucketizerOptions);
        this.pageNumber = 0;
        this.lastTimestamp = new Date(Date.parse("2022-04-07T08:08:48Z"));
        this.period = bucketizerOptions.period || (5 * 60 * 1000); // Default period of 5 minutes
        this.minTimestamp = bucketizerOptions.minTimestamp;
        this.existingBuckets = []
        this.bucketCounter = 0;

        if (state) {
            this.importState(state);
        }
    }

    public getRoot(): string {
        return "0";
    }

    public static build(options: TimeInputType, state?: any): TimeBucketizer {
        return new TimeBucketizer(options, state);
    }

    public bucketize = (quads: RDF.Quad[], memberId: string): RDF.Quad[] => {
        const out: RDF.Quad[] = [];
        const quadTimestamp = this.getQuadTimestamp(quads);
        if (quadTimestamp && quadTimestamp.getTime() > this.lastTimestamp.getTime() + this.period) {
            const currentPage = this.pageNumber;
            this.increasePageNumber();
            this.setLastTimestamp(quadTimestamp);

            const parameters = this.createRelationParameters(this.pageNumber, this.lastTimestamp, this.factory.namedNode('http://www.w3.org/ns/sosa/resultTime'));
            this.setHypermediaControls(`${currentPage}`, parameters);
            out.push(...this.expandRelation(`${currentPage}`, parameters));
        }
        out.push(...this.createSDSRecord(this.factory.namedNode(memberId), [`${this.pageNumber}`]));
        return out;
    };
    public bucketizeNonChronological(quads: RDF.Quad[], memberId: string, minTimestamp: Date): RDF.Quad[]{
        const out: RDF.Quad[] = [];
        const quadTimestamp = this.getQuadTimestamp(quads);
        if (quadTimestamp) {
            const quadTime = quadTimestamp.getTime();
            const lastTime = this.lastTimestamp.getTime();
            const timeDiff = quadTime - lastTime;
            if (timeDiff > 0) {
                const bucketsToCreate = Math.floor(timeDiff / this.period);
                for (let i = 0; i < bucketsToCreate + 1; i++) {
                    const bucketStart = new Date(minTimestamp.getTime() + (this.bucketCounter * this.period));
                    const bucketEnd = new Date(minTimestamp.getTime() + (this.bucketCounter * this.period) + this.period);
                    this.increasePageNumber();
                    const currentPage = this.pageNumber;
                    const parameters = this.createRelationParameters(currentPage, bucketStart, this.factory.namedNode('http://www.w3.org/ns/sosa/resultTime'));
                    this.setHypermediaControls(`${currentPage}`, parameters);
                    out.push(...this.expandRelation(`${currentPage}`, parameters));
                    const newBucket: timeBasedBucket = {
                        startTime: bucketStart,
                        endTime: bucketEnd,
                        pageNumber: currentPage
                    };
                    this.existingBuckets.push(newBucket);
                    this.increaseBucketNumber();
                    }
                console.log((minTimestamp.getTime() + (this.bucketCounter * this.period)) + "BucketStart")
                console.log(quadTimestamp + "added member")
                out.push(...this.createSDSRecord(this.factory.namedNode(memberId), [`${this.pageNumber}`]));
                this.setLastTimestamp(new Date(minTimestamp.getTime() * this.bucketCounter * this.period + bucketsToCreate * this.period));
            } else {
                const existingBucketIndex = this.findExistingBucketIndex(quadTimestamp);
                if (existingBucketIndex !== -1) {
                    const existingBucket = this.existingBuckets[existingBucketIndex];
                    const addPage = existingBucket.pageNumber;
                    out.push(...this.createSDSRecord(this.factory.namedNode(memberId), [`${addPage}`]));
                }
                console.log((this.existingBuckets[existingBucketIndex].startTime) + "BucketStart")
                console.log(quadTimestamp + "added member")
            }
        }
        return out;
    }
    private findExistingBucketIndex(quadTimestamp: Date): number {
        for (let i = 0; i < this.existingBuckets.length; i++) {
            const bucket = this.existingBuckets[i];
            if (bucket.startTime <= quadTimestamp && quadTimestamp < bucket.endTime) {
                return i;
            }
        }
        return -1;
    }
    public exportState = (): any => {
        const state = super.exportState();
        state.lastTimestamp = this.lastTimestamp;
        return state;
    };

    public importState = (state: any): void => {
        super.importState(state);
        this.lastTimestamp = state.lastTimestamp;
    };
    private readonly increasePageNumber = (): number => this.pageNumber++;

    private readonly increaseBucketNumber = (): number => this.bucketCounter++;


    private readonly setLastTimestamp = (timestamp: Date): void => {
        this.lastTimestamp = timestamp;
    };
    private readonly getQuadTimestamp = (quads: RDF.Quad | RDF.Quad[]): Date | undefined => {
        const quadArray = Array.isArray(quads) ? quads : [quads];
        const quad = quadArray.find(q => q.predicate && q.predicate.value === "http://www.w3.org/ns/sosa/resultTime");
        if (quad && quad.object.termType === 'Literal') {
            return new Date(quad.object.value);
        } else {
            return undefined;
        }
    };
    private readonly createRelationParameters = (
        targetNode: number,
        value: Date,
        pathObject: RDF.Term
    ): RelationParameters => ({
        nodeId: targetNode.toString(),
        type: RelationType.GreaterThanOrEqualTo,
        value: [this.factory.literal(value.toISOString(), this.factory.namedNode('http://www.w3.org/2001/XMLSchema#dateTime'))],
        path: pathObject,
    });
}