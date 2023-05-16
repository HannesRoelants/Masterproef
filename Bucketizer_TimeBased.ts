import type * as RDF from '@rdfjs/types';
import { Factory, parseBucketizerCoreOptions, Partial } from '@treecg/bucketizer-core';
import { BucketizerCore } from '@treecg/bucketizer-core';
import { Bucketizer, BucketizerCoreOptions, RelationParameters } from '@treecg/types';
import { RelationType, LDES } from '@treecg/types';


export class TimeBucketizerFactory implements Factory<BucketizerCoreOptions> {
    type: string = "time";
    build(config: BucketizerCoreOptions, state?: any): Bucketizer {
        const total = Object.assign(config, { lastTimestamp: new Date().getTime() })
        return TimeBucketizer.build(total, state);
    }

    ldConfig(quads: RDF.Quad[], subject: RDF.Term): BucketizerCoreOptions | void {
        const out = parseBucketizerCoreOptions(quads, subject);
        if(out.type.value === LDES.custom(this.type)) {
            return out;
        } else {
            return;
        }
    }
}

export type TimeInputType = Partial<BucketizerCoreOptions,  { lastTimestamp: number}>;
export class TimeBucketizer extends BucketizerCore<{ lastTimestamp: number}> {
    public pageNumber: number;
    public lastTimestamp: Date;
    public memberCounter: number;

    private constructor(bucketizerOptions: TimeInputType, state?: any) {
        super(bucketizerOptions);
        this.pageNumber = 0;
        this.lastTimestamp = new Date(Date.parse("2022-04-07T08:08:48Z"));
        this.memberCounter = 0;

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
        console.log(this.lastTimestamp)
        if (quadTimestamp && quadTimestamp.getTime() > this.lastTimestamp.getTime() /*+ 3600000 */) {
            const currentPage = this.pageNumber;
            this.increasePageNumber();
            this.resetMemberCounter();
            this.setLastTimestamp(quadTimestamp);

            const parameters = this.createRelationParameters(this.pageNumber, this.lastTimestamp, this.factory.namedNode('http://www.w3.org/ns/sosa/resultTime'));
            this.setHypermediaControls(`${currentPage}`, parameters);
            out.push(...this.expandRelation(`${currentPage}`, parameters));
        }

        this.increaseMemberCounter();

        out.push(...this.createSDSRecord(this.factory.namedNode(memberId), [`${this.pageNumber}`]));
        return out;
    };

    public exportState = (): any => {
        const state = super.exportState();
        state.lastTimestamp = this.lastTimestamp;
        state.memberCounter = this.memberCounter;
        return state;
    };

    public importState = (state: any): void => {
        super.importState(state);
        this.lastTimestamp = state.lastTimestamp;
        this.memberCounter = state.memberCounter;
    };

    private readonly increasePageNumber = (): number => this.pageNumber++;

    private readonly increaseMemberCounter = (): number => this.memberCounter++;

    private readonly resetMemberCounter = (): void => {
        this.memberCounter = 0;
    };

    private readonly setLastTimestamp = (timestamp : Date): void=>{
        this.lastTimestamp = timestamp;
    };

    private readonly getQuadTimestamp = (quads: RDF.Quad[]): Date | undefined => {
        const quad = quads.find(q => q.predicate && q.predicate.value === "http://www.w3.org/ns/sosa/resultTime");
        if (quad && quad.object.termType === 'Literal'){
            return new Date(quad.object.value);
        } else {
            return undefined;
        }
    }
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